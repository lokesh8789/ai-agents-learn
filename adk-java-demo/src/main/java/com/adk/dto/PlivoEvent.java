package com.adk.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Builder;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "event")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PlivoEvent.Start.class, name = "start"),
        @JsonSubTypes.Type(value = PlivoEvent.Media.class, name = "media"),
        @JsonSubTypes.Type(value = PlivoEvent.Clear.class, name = "clearAudio"),
        @JsonSubTypes.Type(value = PlivoEvent.PlayAudio.class, name = "playAudio"),
        @JsonSubTypes.Type(value = PlivoEvent.Cleared.class, name = "clearedAudio"),
})
public sealed interface PlivoEvent {
    String event();

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Start(
            String sequenceNumber,
            StartData start,
            @JsonProperty("extra_headers") String extraHeaders
    ) implements PlivoEvent {
        @Override
        public String event() {
            return "start";
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public record StartData(
                String streamId,
                String callId,
                String accountId,
                List<String> tracks,
                MediaFormat mediaFormat
        ) {
            @JsonIgnoreProperties(ignoreUnknown = true)
            public record MediaFormat(
                    String encoding,
                    Integer sampleRate
            ) {
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Builder
    record PlayAudio(
            MediaData media
    ) implements PlivoEvent {
        @Override
        public String event() {
            return "playAudio";
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        @Builder
        public record MediaData(
                Integer sampleRate,
                String contentType,
                String payload
        ) { }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Media(
            String streamId,
            String sequenceNumber,
            MediaData media,
            @JsonProperty("extra_headers") String extraHeaders
    ) implements PlivoEvent {
        @Override
        public String event() {
            return "media";
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public record MediaData(
                String track,
                String chunk,
                String timestamp,
                String payload
        ) { }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Builder
    record Clear(
            String streamId
    ) implements PlivoEvent {
        @Override
        public String event() {
            return "clearAudio";
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Cleared(
            String streamId,
            String sequenceNumber
    ) implements PlivoEvent {
        @Override
        public String event() {
            return "clearedAudio";
        }
    }

}
