package com.panopticum.i18n;

import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class MessagesMongo {

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
