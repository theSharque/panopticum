package com.panopticum.i18n;

import java.util.Map;

public final class MessagesCassandra {

    private MessagesCassandra() {
    }

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("cassandra.durableWrites", "Durable writes"),
            Map.entry("cassandra.replication", "Replication"),
            Map.entry("cassandra.comment", "Comment"),
            Map.entry("cassandra.defaultTtl", "Default TTL"),
            Map.entry("cassandra.gcGraceSeconds", "GC grace (s)")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("cassandra.durableWrites", "Устойчивая запись"),
            Map.entry("cassandra.replication", "Репликация"),
            Map.entry("cassandra.comment", "Комментарий"),
            Map.entry("cassandra.defaultTtl", "TTL по умолчанию"),
            Map.entry("cassandra.gcGraceSeconds", "GC grace (с)")
    );
}
