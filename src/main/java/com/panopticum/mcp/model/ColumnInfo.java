package com.panopticum.mcp.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@Serdeable
public class ColumnInfo {

    private final String name;
    private final String type;
    private final boolean nullable;
    private final boolean primaryKey;
    private final int position;
}
