package com.panopticum.i18n;

import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class MessagesLightJdbc {

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("lightjdbc.schemasTitle", "Schemas"),
            Map.entry("lightjdbc.tablesTitle", "Tables and views"),
            Map.entry("settings.placeholderNameH2", "e.g. Local H2 server"),
            Map.entry("settings.placeholderNameHsqldb", "e.g. Local HyperSQL"),
            Map.entry("settings.placeholderNameDerby", "e.g. Local Derby network"),
            Map.entry("settings.lightJdbcDbHelp", "Database path or name (as in JDBC URL)")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("lightjdbc.schemasTitle", "Схемы"),
            Map.entry("lightjdbc.tablesTitle", "Таблицы и представления"),
            Map.entry("settings.placeholderNameH2", "Например: Local H2 server"),
            Map.entry("settings.placeholderNameHsqldb", "Например: Local HyperSQL"),
            Map.entry("settings.placeholderNameDerby", "Например: Local Derby network"),
            Map.entry("settings.lightJdbcDbHelp", "Путь или имя БД (как в JDBC URL)")
    );
}
