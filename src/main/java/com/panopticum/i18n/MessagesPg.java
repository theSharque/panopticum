package com.panopticum.i18n;

import java.util.Map;

public final class MessagesPg {

    private MessagesPg() {
    }

    public static final Map<String, String> EN = Map.ofEntries(
            Map.entry("pg.tablesAndViews", "Tables and views"),
            Map.entry("pg.rowsApprox", "Rows"),
            Map.entry("pg.schemas", "Schemas"),
            Map.entry("pg.owner", "Owner"),
            Map.entry("pg.tableCount", "Tables"),
            Map.entry("pg.rowDetail", "Row details")
    );

    public static final Map<String, String> RU = Map.ofEntries(
            Map.entry("pg.tablesAndViews", "Таблицы и представления"),
            Map.entry("pg.rowsApprox", "Строк"),
            Map.entry("pg.schemas", "Схемы"),
            Map.entry("pg.owner", "Владелец"),
            Map.entry("pg.tableCount", "Таблиц"),
            Map.entry("pg.rowDetail", "Детали строки")
    );
}
