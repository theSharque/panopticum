package com.panopticum.mcp.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@Serdeable
public class EntityDescription {

    private final Long connectionId;
    private final String dbType;
    private final String entityKind;
    private final String catalog;
    private final String namespace;
    private final String entity;
    private final List<ColumnInfo> columns;
    private final List<String> primaryKey;
    private final List<ForeignKeyInfo> foreignKeys;
    private final List<IndexInfo> indexes;
    private final Long approximateRowCount;
    private final boolean inferredFromSample;
    private final List<String> notes;
}
