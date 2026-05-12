package com.panopticum.i18n;

import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class MessagesCouchbase {

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("couchbase.bucketsTitle", "Couchbase buckets"),
            Map.entry("couchbase.collectionsTitle", "Scopes and collections"),
            Map.entry("couchbase.documentsTitle", "Documents"),
            Map.entry("couchbase.documentTitle", "Document"),
            Map.entry("couchbase.n1qlTitle", "N1QL query"),
            Map.entry("couchbase.n1qlEditor", "N1QL editor"),
            Map.entry("couchbase.n1qlPlaceholder", "SELECT META().id, b.* FROM `bucket`.`scope`.`collection` AS b LIMIT 10"),
            Map.entry("couchbase.scope", "Scope"),
            Map.entry("couchbase.collection", "Collection"),
            Map.entry("couchbase.ramQuota", "RAM quota (MB)"),
            Map.entry("couchbase.noBuckets", "No buckets or connection failed."),
            Map.entry("couchbase.noCollections", "No collections or connection failed."),
            Map.entry("settings.placeholderNameCouchbase", "e.g. Local Couchbase"),
            Map.entry("settings.couchbaseHostHelp", "Bootstrap host or full connection string (couchbase://...)"),
            Map.entry("settings.couchbaseTls", "TLS (couchbases://)"),
            Map.entry("settings.couchbaseBucketHelp", "Default bucket (optional)")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("couchbase.bucketsTitle", "Бакеты Couchbase"),
            Map.entry("couchbase.collectionsTitle", "Области и коллекции"),
            Map.entry("couchbase.documentsTitle", "Документы"),
            Map.entry("couchbase.documentTitle", "Документ"),
            Map.entry("couchbase.n1qlTitle", "Запрос N1QL"),
            Map.entry("couchbase.n1qlEditor", "Редактор N1QL"),
            Map.entry("couchbase.n1qlPlaceholder", "SELECT META().id, b.* FROM `bucket`.`scope`.`collection` AS b LIMIT 10"),
            Map.entry("couchbase.scope", "Область"),
            Map.entry("couchbase.collection", "Коллекция"),
            Map.entry("couchbase.ramQuota", "Квота RAM (МБ)"),
            Map.entry("couchbase.noBuckets", "Нет бакетов или ошибка подключения."),
            Map.entry("couchbase.noCollections", "Нет коллекций или ошибка подключения."),
            Map.entry("settings.placeholderNameCouchbase", "Например: Local Couchbase"),
            Map.entry("settings.couchbaseHostHelp", "Хост bootstrap или полная строка подключения (couchbase://...)"),
            Map.entry("settings.couchbaseTls", "TLS (couchbases://)"),
            Map.entry("settings.couchbaseBucketHelp", "Бакет по умолчанию (необязательно)")
    );
}
