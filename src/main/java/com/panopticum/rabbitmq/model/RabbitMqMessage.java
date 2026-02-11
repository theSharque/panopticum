package com.panopticum.rabbitmq.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@Serdeable
public class RabbitMqMessage {

    private String payload;

    @JsonProperty("payload_bytes")
    private Long payloadBytes;

    private Object properties;

    @JsonProperty("routing_key")
    private String routingKey;
}

