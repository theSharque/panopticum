package com.panopticum.i18n;

import java.util.Map;

public final class MessagesRedis {

    private MessagesRedis() {
    }

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("redis.dbNumber", "DB number"),
            Map.entry("redis.keyCount", "Keys"),
            Map.entry("redis.keysTitle", "Keys"),
            Map.entry("redis.searchPlaceholder", "Key contains"),
            Map.entry("redis.toStart", "To start"),
            Map.entry("redis.key", "Key"),
            Map.entry("redis.ttl", "TTL"),
            Map.entry("redis.keyDetailTitle", "Key"),
            Map.entry("redis.noExpiry", "no expiry"),
            Map.entry("redis.keyNotFoundOrError", "Key not found or connection error.")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("redis.dbNumber", "Номер БД"),
            Map.entry("redis.keyCount", "Ключей"),
            Map.entry("redis.keysTitle", "Ключи"),
            Map.entry("redis.searchPlaceholder", "По ключу"),
            Map.entry("redis.toStart", "В начало"),
            Map.entry("redis.key", "Ключ"),
            Map.entry("redis.ttl", "TTL"),
            Map.entry("redis.keyDetailTitle", "Ключ"),
            Map.entry("redis.noExpiry", "без срока"),
            Map.entry("redis.keyNotFoundOrError", "Ключ не найден или ошибка подключения.")
    );
}
