package com.panopticum.i18n;

import java.util.Map;

public final class MessagesElasticsearch {

    private MessagesElasticsearch() {
    }

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("elasticsearch.indices", "Indices"),
            Map.entry("elasticsearch.search", "Search"),
            Map.entry("elasticsearch.documentDetail", "Document details"),
            Map.entry("elasticsearch.index", "Index"),
            Map.entry("elasticsearch.docsCount", "Docs"),
            Map.entry("elasticsearch.storeSize", "Size"),
            Map.entry("elasticsearch.queryPlaceholder", "Query DSL (e.g. {\"query\":{\"match_all\":{}}})"),
            Map.entry("error.specifyIndex", "Specify index."),
            Map.entry("error.invalidJson", "Invalid JSON.")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("elasticsearch.indices", "Индексы"),
            Map.entry("elasticsearch.search", "Поиск"),
            Map.entry("elasticsearch.documentDetail", "Детали документа"),
            Map.entry("elasticsearch.index", "Индекс"),
            Map.entry("elasticsearch.docsCount", "Документов"),
            Map.entry("elasticsearch.storeSize", "Размер"),
            Map.entry("elasticsearch.queryPlaceholder", "Query DSL (напр. {\"query\":{\"match_all\":{}}})"),
            Map.entry("error.specifyIndex", "Укажите индекс."),
            Map.entry("error.invalidJson", "Неверный JSON.")
    );
}
