package com.panopticum.redis;

import lombok.experimental.UtilityClass;

@UtilityClass
public class RedisScanCursors {

    public String normalize(String cursor) {
        if (cursor == null) {
            return "0";
        }
        String t = cursor.trim();
        return t.isEmpty() ? "0" : t;
    }

    public boolean isAtStart(String normalizedCursor) {
        return "0".equals(normalizedCursor);
    }
}
