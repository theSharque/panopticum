package com.panopticum.core.model;

import lombok.Getter;

import java.util.List;

@Getter
public class QueryResult {

    private final List<String> columns;
    private final List<String> columnTypes;
    private final List<List<Object>> rows;
    private final List<String> docIds;
    private final String error;
    private final int offset;
    private final int limit;
    private final boolean hasMore;

    public QueryResult(List<String> columns, List<List<Object>> rows, String error, int offset, int limit, boolean hasMore) {
        this(columns, null, rows, null, error, offset, limit, hasMore);
    }

    public QueryResult(List<String> columns, List<String> columnTypes, List<List<Object>> rows, List<String> docIds,
                       String error, int offset, int limit, boolean hasMore) {
        this.columns = columns != null ? columns : List.of();
        this.columnTypes = columnTypes;
        this.rows = rows != null ? rows : List.of();
        this.docIds = docIds;
        this.error = error;
        this.offset = offset;
        this.limit = limit;
        this.hasMore = hasMore;
    }

    public static QueryResult error(String message) {
        return new QueryResult(List.of(), List.of(), message, 0, 0, false);
    }

    public boolean hasError() {
        return error != null;
    }

    public boolean hasPrev() {
        return offset > 0;
    }

    public int nextOffset() {
        return offset + limit;
    }

    public int prevOffset() {
        return Math.max(0, offset - limit);
    }

    public int fromRow() {
        return offset + 1;
    }

    public int toRow() {
        return offset + rows.size();
    }
}
