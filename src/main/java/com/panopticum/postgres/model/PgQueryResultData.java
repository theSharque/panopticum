package com.panopticum.postgres.model;

import lombok.Value;

import java.util.List;

@Value
public class PgQueryResultData {

    private final List<String> columns;
    private final List<String> columnTypes;
    private final List<List<Object>> rows;
}
