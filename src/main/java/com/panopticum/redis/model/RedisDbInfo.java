package com.panopticum.redis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RedisDbInfo {

    private int dbIndex;
    private long keyCount;
}
