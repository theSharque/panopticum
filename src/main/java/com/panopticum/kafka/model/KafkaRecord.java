package com.panopticum.kafka.model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class KafkaRecord {

    private long offset;
    private int partition;
    private String key;
    private String value;
    private Long timestamp;
    private Map<String, String> headers;
}
