package com.panopticum.i18n;

import java.util.Map;

public final class MessagesSettings {

    private MessagesSettings() {
    }

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("settings.title", "Add connection"),
            Map.entry("settings.connectionType", "Connection type"),
            Map.entry("settings.selectType", "Select type"),
            Map.entry("settings.host", "Host"),
            Map.entry("settings.port", "Port"),
            Map.entry("settings.database", "Database"),
            Map.entry("settings.databaseRedis", "DB (0–15)"),
            Map.entry("settings.username", "Username"),
            Map.entry("settings.password", "Password"),
            Map.entry("settings.test", "Test"),
            Map.entry("settings.testing", "Checking…"),
            Map.entry("settings.add", "Add"),
            Map.entry("settings.addedConnections", "Saved connections"),
            Map.entry("settings.connectionsInMenu", "Connections are shown in the menu on the left."),
            Map.entry("settings.placeholderNamePg", "e.g. Production DB"),
            Map.entry("settings.placeholderNameMongo", "e.g. Production Mongo"),
            Map.entry("settings.placeholderNameRedis", "e.g. Production Redis"),
            Map.entry("settings.placeholderNameCh", "e.g. Production ClickHouse"),
            Map.entry("settings.placeholderNameMysql", "e.g. Production MySQL"),
            Map.entry("settings.placeholderNameMssql", "e.g. Production MS SQL"),
            Map.entry("settings.placeholderNameOracle", "e.g. Production Oracle"),
            Map.entry("settings.placeholderNameCassandra", "e.g. Production Cassandra"),
            Map.entry("settings.placeholderNameRabbitmq", "e.g. Production RabbitMQ"),
            Map.entry("settings.vhost", "VHost")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("settings.title", "Добавить подключение"),
            Map.entry("settings.connectionType", "Тип подключения"),
            Map.entry("settings.selectType", "Выберите тип"),
            Map.entry("settings.host", "Хост"),
            Map.entry("settings.port", "Порт"),
            Map.entry("settings.database", "База данных"),
            Map.entry("settings.databaseRedis", "База (0–15)"),
            Map.entry("settings.username", "Пользователь"),
            Map.entry("settings.password", "Пароль"),
            Map.entry("settings.test", "Проверить"),
            Map.entry("settings.testing", "Проверка…"),
            Map.entry("settings.add", "Добавить"),
            Map.entry("settings.addedConnections", "Добавленные подключения"),
            Map.entry("settings.connectionsInMenu", "Список подключений отображается в меню слева."),
            Map.entry("settings.placeholderNamePg", "Например: Production DB"),
            Map.entry("settings.placeholderNameMongo", "Например: Production Mongo"),
            Map.entry("settings.placeholderNameRedis", "Например: Production Redis"),
            Map.entry("settings.placeholderNameCh", "Например: Production ClickHouse"),
            Map.entry("settings.placeholderNameMysql", "Например: Production MySQL"),
            Map.entry("settings.placeholderNameMssql", "Например: Production MS SQL"),
            Map.entry("settings.placeholderNameOracle", "Например: Production Oracle"),
            Map.entry("settings.placeholderNameCassandra", "Например: Production Cassandra"),
            Map.entry("settings.placeholderNameRabbitmq", "Например: Production RabbitMQ"),
            Map.entry("settings.vhost", "VHost")
    );
}
