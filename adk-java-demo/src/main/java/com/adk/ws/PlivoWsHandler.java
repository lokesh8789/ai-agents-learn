package com.adk.ws;

import com.adk.config.GeminiProperties;
import com.adk.dto.PlivoEvent;
import com.adk.dto.PlivoEvent.*;
import com.adk.dto.PlivoEvent.PlayAudio.MediaData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.agents.LiveRequestQueue;
import com.google.adk.agents.LlmAgent;
import com.google.adk.agents.RunConfig;
import com.google.adk.events.Event;
import com.google.adk.models.Gemini;
import com.google.adk.runner.InMemoryRunner;
import com.google.genai.types.*;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Base64;
import java.util.List;
import java.util.function.Function;

import static reactor.adapter.rxjava.RxJava3Adapter.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class PlivoWsHandler implements WebSocketHandler {

    private final GeminiProperties geminiProperties;
    private final ObjectMapper objectMapper;

    private static final String APP_NAME = "car_agent";
    private static final String DEFAULT_MODEL = "gemini-2.5-flash-native-audio-preview-12-2025";

    private LlmAgent createAgent() {
        return LlmAgent.builder()
                .instruction("Your name is John and your job is to assist users about cars")
                .name(APP_NAME)
                .model(Gemini.builder()
                        .modelName(DEFAULT_MODEL)
                        .apiKey(geminiProperties.apiKey())
                        .build())
                .build();
    }

    @Builder(toBuilder = true)
    public record AgentSession(Flux<Event> agentEvents, LiveRequestQueue liveRequestQueue) {}

    @Override
    @NonNull
    public Mono<Void> handle(@NonNull WebSocketSession ws) {
        log.info("PlivoWsHandler handle");
        AgentSession agentSession = startAgentSession(ws);
        return Mono.when(
                agentToClient(ws, agentSession.agentEvents())
                        .doFinally(signalType -> agentSession.liveRequestQueue().close()),
                clientToAgent(ws, agentSession.liveRequestQueue())
                        .doFinally(signalType -> agentSession.liveRequestQueue().close())
        );
    }

    public AgentSession startAgentSession(WebSocketSession ws) {
        LlmAgent agent = createAgent();
        InMemoryRunner runner = new InMemoryRunner(agent, APP_NAME);

        LiveRequestQueue liveRequestQueue = new LiveRequestQueue();

        return AgentSession.builder()
                .liveRequestQueue(liveRequestQueue)
                .agentEvents(
                        singleToMono(
                                runner.sessionService()
                                        .createSession(APP_NAME, "user-" + ws.getId())
                        ).flatMapMany(session -> {
                            RunConfig runConfig = RunConfig.builder()
                                    .setResponseModalities(List.of(new Modality(Modality.Known.AUDIO)))
                                    .setStreamingMode(RunConfig.StreamingMode.BIDI)
                                    .setSpeechConfig(SpeechConfig.builder()
                                            .voiceConfig(VoiceConfig.builder()
                                                    .prebuiltVoiceConfig(PrebuiltVoiceConfig.builder()
                                                            .voiceName("Kore")
                                                            .build())
                                                    .build())
                                            .build())
                                    .build();
                            return flowableToFlux(runner.runLive(session, liveRequestQueue, runConfig));
                        }))
                .build();
    }

    public Mono<Void> agentToClient(WebSocketSession ws, Flux<Event> agentEvents) {
        return agentEvents
                .doOnNext(event -> log.info("agentToClient Interrupted event: {}", event.interrupted().orElse(null)))
                .flatMap(event -> Mono.justOrEmpty(event.interrupted())
                        .doOnNext(interruptedEvent -> log.info("agentToClient interruptedEvent: {}", interruptedEvent))
                        .filter(BooleanUtils::isTrue)
                        .mapNotNull(message -> {
                            Object streamId = ws.getAttributes().get("streamId");
                            var clear = Clear.builder()
                                    .streamId(String.valueOf(streamId))
                                    .build();
                            try {
                                log.info("Clearing Message: {}", clear);
                                return ws.textMessage(objectMapper.writeValueAsString(clear));
                            } catch (JsonProcessingException e) {
                                log.info("Failed to serialize clear event", e);
                                return null;
                            }
                        })
                        .flux()
                        .switchIfEmpty(Mono.justOrEmpty(event.content())
                                .flatMapMany(content -> Mono.justOrEmpty(content.parts()))
                                .flatMapIterable(Function.identity())
                                .flatMap(part -> Mono.justOrEmpty(part.inlineData()))
                                .flatMap(blob -> Mono.justOrEmpty(blob.data()))
                                .mapNotNull(bytes -> {
                                    var playAudio = PlayAudio.builder()
                                            .media(MediaData.builder()
                                                    .payload(Base64.getEncoder().encodeToString(bytes))
                                                    .contentType("audio/x-l16")
                                                    .sampleRate(24000)
                                                    .build())
                                            .build();
                                    try {
                                        return ws.textMessage(objectMapper.writeValueAsString(playAudio));
                                    } catch (JsonProcessingException e) {
                                        log.info("Failed to serialize playAudio event", e);
                                        return null;
                                    }
                                })
                        )
                ).as(ws::send);
    }

    public Mono<Void> clientToAgent(WebSocketSession ws, LiveRequestQueue liveRequestQueue) {
        return ws.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .mapNotNull(data -> {
                    try {
                        PlivoEvent plivoEvent = objectMapper.readValue(data, PlivoEvent.class);
                        switch (plivoEvent) {
                            case Start start -> {
                                String streamId = start.start().streamId();
                                ws.getAttributes().put("streamId", streamId);
                                log.info("PlivoWsHandler client started: {}", start);
                            }
                            case Cleared cleared -> log.info("PlivoWsHandler client cleared: {}", cleared.streamId());
                            case Media media -> {
                                String payload = media.media().payload();
                                byte[] bytes = Base64.getDecoder().decode(payload);
                                Blob blob = Blob.builder()
                                        .mimeType("audio/pcm;rate=16000")
                                        .data(bytes)
                                        .build();
                                liveRequestQueue.realtime(blob);
                            }
                            default -> {}
                        }
                    } catch (JsonProcessingException e) {
                        log.info("Json Error", e);
                        return null;
                    }
                    return null;
                })
                .then();
    }

}
