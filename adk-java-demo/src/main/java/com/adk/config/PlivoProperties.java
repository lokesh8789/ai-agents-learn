package com.adk.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "plivo")
public record PlivoProperties(String authKey, String authToken) {}
