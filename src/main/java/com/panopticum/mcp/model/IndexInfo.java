package com.panopticum.mcp.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@Serdeable
public class IndexInfo {

    private final String name;
    private final boolean unique;
    private final List<String> columns;
}
