package com.panopticum.mcp.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
@Serdeable
public class ForeignKeyInfo {

    private final List<String> columns;
    private final Map<String, Object> references;
}
