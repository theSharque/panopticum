package com.panopticum.i18n;

import java.util.Map;

public final class MessagesKafka {

    private MessagesKafka() {
    }

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("kafka.topicsTitle", "Topics"),
            Map.entry("kafka.partitionsTitle", "Partitions"),
            Map.entry("kafka.partitions", "Partitions"),
            Map.entry("kafka.recordsTitle", "Records"),
            Map.entry("kafka.recordDetailTitle", "Record"),
            Map.entry("kafka.topic", "Topic"),
            Map.entry("kafka.partition", "Partition"),
            Map.entry("kafka.offset", "Offset"),
            Map.entry("kafka.key", "Key"),
            Map.entry("kafka.value", "Value"),
            Map.entry("kafka.timestamp", "Timestamp"),
            Map.entry("kafka.headers", "Headers"),
            Map.entry("kafka.noTopics", "No topics or connection error."),
            Map.entry("kafka.noPartitions", "No partitions or connection error."),
            Map.entry("kafka.noRecords", "No records or connection error."),
            Map.entry("kafka.recordNotFound", "Record not found."),
            Map.entry("kafka.refresh", "Refresh")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("kafka.topicsTitle", "Топики"),
            Map.entry("kafka.partitionsTitle", "Партиции"),
            Map.entry("kafka.partitions", "Партиции"),
            Map.entry("kafka.recordsTitle", "Записи"),
            Map.entry("kafka.recordDetailTitle", "Запись"),
            Map.entry("kafka.topic", "Топик"),
            Map.entry("kafka.partition", "Партиция"),
            Map.entry("kafka.offset", "Offset"),
            Map.entry("kafka.key", "Ключ"),
            Map.entry("kafka.value", "Значение"),
            Map.entry("kafka.timestamp", "Время"),
            Map.entry("kafka.headers", "Заголовки"),
            Map.entry("kafka.noTopics", "Нет топиков или ошибка подключения."),
            Map.entry("kafka.noPartitions", "Нет партиций или ошибка подключения."),
            Map.entry("kafka.noRecords", "Нет записей или ошибка подключения."),
            Map.entry("kafka.recordNotFound", "Запись не найдена."),
            Map.entry("kafka.refresh", "Обновить")
    );
}
