package com.panopticum.redis.model;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;

@FunctionalInterface
public interface RedisConnectionCallback<T> {

    T apply(RedisCommands<String, String> cmd, RedisServerCommands<String, String> dbCmd);
}
