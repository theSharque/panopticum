package com.panopticum.redis.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
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
