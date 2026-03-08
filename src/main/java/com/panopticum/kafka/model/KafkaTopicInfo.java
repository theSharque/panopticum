package com.panopticum.kafka.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Data;

@Data
@Serdeable
public class KafkaTopicInfo {

    private String name;
    private int partitionCount;
}
