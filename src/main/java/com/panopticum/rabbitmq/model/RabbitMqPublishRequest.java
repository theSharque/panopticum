package com.panopticum.rabbitmq.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class RabbitMqPublishRequest {

    private Map<String, Object> properties;

    @JsonProperty("routing_key")
    private String routingKey;

    private String payload;

    @JsonProperty("payload_encoding")
    private String payloadEncoding;
}
