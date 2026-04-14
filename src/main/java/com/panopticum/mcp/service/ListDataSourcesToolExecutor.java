package com.panopticum.mcp.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.mcp.model.McpToolContent;
import com.panopticum.mcp.model.McpToolRequest;
import com.panopticum.mcp.model.McpToolResponse;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class ListDataSourcesToolExecutor implements McpToolExecutor {

    private final DbConnectionService dbConnectionService;
    private final ObjectMapper objectMapper;

    @Override
    public String getToolName() {
        return "list-data-sources";
    }

    @Override
    public McpToolResponse execute(McpToolRequest request) {
        log.debug("Executing list-data-sources");

        List<Map<String, Object>> items = new ArrayList<>();
        for (DbConnection conn : dbConnectionService.findAll()) {
            Map<String, Object> m = new HashMap<>();
            m.put("connectionId", conn.getId());
            m.put("name", conn.getName());
            m.put("dbType", normalizeDbType(conn.getType()));
            m.put("queryFormat", queryFormatFor(conn.getType()));
            m.put("hierarchyModel", hierarchyModelFor(conn.getType()));
            items.add(m);
        }

        Map<String, Object> result = Map.of("items", items);

        try {
            String json = objectMapper.writeValueAsString(result);
            return McpToolResponse.builder()
                    .content(List.of(McpToolContent.builder().type("text").text(json).build()))
                    .isError(false)
                    .build();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize list-data-sources result", e);
            return McpToolResponse.builder()
                    .isError(true)
                    .error("Failed to serialize result: " + e.getMessage())
                    .build();
        }
    }

    private static String normalizeDbType(String type) {
        if (type == null || type.isBlank()) {
            return "";
        }
        return "mssql".equalsIgnoreCase(type) ? "sqlserver" : type.toLowerCase();
    }

    private static String queryFormatFor(String type) {
        if (type == null || type.isBlank()) {
            return "sql";
        }
        return switch (type.toLowerCase()) {
            case "mongodb", "elasticsearch" -> "json";
            case "cassandra" -> "cql";
            case "kafka" -> "json";
            case "kubernetes" -> "tail";
            case "redis" -> "pattern";
            default -> "sql";
        };
    }

    private static String hierarchyModelFor(String type) {
        if (type == null || type.isBlank()) {
            return "catalog.entity";
        }
        return switch (type.toLowerCase()) {
            case "postgresql", "sqlserver" -> "catalog.namespace.entity";
            case "oracle" -> "namespace.entity";
            case "kafka" -> "catalog.entity";
            case "kubernetes" -> "catalog.entity";
            case "redis" -> "catalog";
            case "elasticsearch" -> "catalog";
            default -> "catalog.entity";
        };
    }
}
