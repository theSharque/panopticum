package com.panopticum.redis.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RedisKeyDetail {

    private String key;
    private String type;
    private Long ttl;
    private Object value;
}
