package com.panopticum.rabbitmq.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@Serdeable
@AllArgsConstructor
public class RabbitMqGetMessagesRequest {

    private int count;

    private String ackmode;

    private String encoding;

    private int truncate;
}

