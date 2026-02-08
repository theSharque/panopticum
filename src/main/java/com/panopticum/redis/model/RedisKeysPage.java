package com.panopticum.redis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RedisKeysPage {

    private List<RedisKeyInfo> keys;
    private String nextCursor;
    private boolean hasMore;

    public static RedisKeysPage empty() {
        return new RedisKeysPage(List.of(), "0", false);
    }
}
