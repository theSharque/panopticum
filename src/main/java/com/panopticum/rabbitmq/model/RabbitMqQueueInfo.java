package com.panopticum.rabbitmq.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Data;

import java.util.Map;

@Data
@Serdeable
public class RabbitMqQueueInfo {

    private String name;

    private String vhost;

    private Long messages;

    @JsonProperty("messages_ready")
    private Long messagesReady;

    @JsonProperty("messages_unacknowledged")
    private Long messagesUnacknowledged;

    private Integer consumers;

    @JsonProperty("message_stats")
    private Map<String, Object> messageStats;

    public String getVhostForUrl() {
        if (vhost == null || vhost.isBlank()) {
            return "_";
        }
        return vhost.replace("/", "_");
    }
}

