package com.panopticum.rabbitmq.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class RabbitMqPublishRequest {

    private String properties;

    private String routingKey;

    private String payload;

    private String payloadEncoding;
}
