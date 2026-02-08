package com.panopticum.redis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RedisKeyDetail {

    private String key;
    private String type;
    private Long ttl;
    private Object value;
}
