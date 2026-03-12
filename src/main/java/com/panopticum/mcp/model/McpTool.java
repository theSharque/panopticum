package com.panopticum.mcp.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Serdeable
public class McpTool {

    private String name;
    private String description;
    private Map<String, Object> inputSchema;
}
