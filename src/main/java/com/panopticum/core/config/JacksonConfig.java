package com.panopticum.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class JacksonConfig {

    @Singleton
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
