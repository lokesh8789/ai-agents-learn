package com.adk.controller;

import com.adk.config.PlivoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/call")
@RequiredArgsConstructor
public class CallController {

    private final PlivoProperties plivoProperties;
    private static final String NGROK_URL = "31b88a961bc9.ngrok-free.app";

    @PostMapping("/outbound")
    public Mono<ResponseEntity<String>> callOutbound(@RequestParam String to, @RequestParam  String plivoNumber) {
        log.info("Props: {}", plivoProperties);
        Map<String, String> body = Map.of(
                "from", plivoNumber,
                "to", to,
                "answer_url", "https://"+NGROK_URL+"/api/v1/call/answerUrl?to="+to,
                "answer_method", "GET"
        );

        return WebClient.create("https://api.plivo.com/v1/Account")
                .post()
                .uri("/{authId}/Call/", plivoProperties.authKey())
                .headers(httpHeaders -> httpHeaders.setBasicAuth(plivoProperties.authKey(), plivoProperties.authToken()))
                .bodyValue(body)
                .retrieve()
                .toEntity(String.class);
    }

    @GetMapping("/answerUrl")
    public Mono<ResponseEntity<String>> answerUrl(@RequestParam String to) {
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                   <Response>
                     <Stream streamTimeout="86400" keepCallAlive="true" bidirectional="true" contentType="audio/x-l16;rate=16000" audioTrack="inbound" >
                       wss://%s/ws/plivo?to=%s
                     </Stream>
                   </Response>
                """.formatted(NGROK_URL, to);

        log.info("Answer URL Triggered, {}", xml);

        return Mono.just(ResponseEntity.status(HttpStatus.OK)
                .contentType(MediaType.APPLICATION_XML)
                .body(xml));
    }
}
