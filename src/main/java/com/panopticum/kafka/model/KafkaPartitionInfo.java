package com.panopticum.kafka.model;

import lombok.Data;

@Data
public class KafkaPartitionInfo {

    private String topic;
    private int partition;
}
