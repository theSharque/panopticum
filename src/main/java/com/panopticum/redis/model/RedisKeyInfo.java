package com.panopticum.redis.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class RedisKeyInfo {

    private String key;
    private String type;
    private Long ttl;
}
