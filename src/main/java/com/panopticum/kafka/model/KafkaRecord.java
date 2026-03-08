package com.panopticum.kafka.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
@Serdeable
public class KafkaRecord {

    private long offset;
    private int partition;
    private String key;
    private String value;
    private Long timestamp;
    private Map<String, String> headers;
}
