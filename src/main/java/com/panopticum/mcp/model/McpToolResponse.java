package com.panopticum.mcp.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Serdeable
public class McpToolResponse {

    private List<McpToolContent> content;
    private String error;
    private Boolean isError;
}
