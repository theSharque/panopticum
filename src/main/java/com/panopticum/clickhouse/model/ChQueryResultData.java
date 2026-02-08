package com.panopticum.clickhouse.model;

import lombok.Value;

import java.util.List;

@Value
public class ChQueryResultData {

    private final List<String> columns;
    private final List<String> columnTypes;
    private final List<List<Object>> rows;
}
