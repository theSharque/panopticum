package com.panopticum.kafka.model;

import lombok.Data;

@Data
public class KafkaTopicInfo {

    private String name;
    private int partitionCount;
}
