package com.panopticum.kafka.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Data;

@Data
@Serdeable
public class KafkaPartitionInfo {

    private String topic;
    private int partition;
}
