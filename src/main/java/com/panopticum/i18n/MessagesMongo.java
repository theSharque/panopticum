package com.panopticum.i18n;

import java.util.Map;

public final class MessagesMongo {

    private MessagesMongo() {
    }

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("mongo.collections", "Collections"),
            Map.entry("mongo.documents", "Documents"),
            Map.entry("mongo.documentDetail", "Document details")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("mongo.collections", "Коллекции"),
            Map.entry("mongo.documents", "Документов"),
            Map.entry("mongo.documentDetail", "Детали документа")
    );
}
