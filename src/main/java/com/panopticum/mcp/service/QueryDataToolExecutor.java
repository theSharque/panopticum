package com.panopticum.mcp.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.core.model.QueryResult;
import com.panopticum.mcp.model.McpToolContent;
import com.panopticum.mcp.model.McpToolRequest;
import com.panopticum.mcp.model.McpToolResponse;
import com.panopticum.mcp.model.UnifiedQueryEnvelope;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class QueryDataToolExecutor implements McpToolExecutor {

    private static final int HARD_LIMIT = 100;

    private final MetadataFacadeService metadataFacadeService;
    private final ObjectMapper objectMapper;

    @Override
    public String getToolName() {
        return "query-data";
    }

    @Override
    public McpToolResponse execute(McpToolRequest request) {
        Map<String, Object> args = request.getArguments() != null ? request.getArguments() : Map.of();
        Long connectionId = toLong(args.get("connectionId"));
        if (connectionId == null) {
            return error("connectionId (number) is required");
        }

        String query = args.get("query") != null ? args.get("query").toString() : null;
        if (query == null || query.isBlank()) {
            return error("query (string) is required");
        }

        String catalog = args.get("catalog") != null ? args.get("catalog").toString() : null;
        String namespace = args.get("namespace") != null ? args.get("namespace").toString() : null;
        String entity = args.get("entity") != null ? args.get("entity").toString() : null;
        int offset = Math.max(0, toInt(args.get("offset"), 0));
        int requestedLimit = toInt(args.get("limit"), 100);
        int effectiveLimit = Math.min(Math.max(1, requestedLimit), HARD_LIMIT);
        String sort = args.get("sort") != null ? args.get("sort").toString() : null;
        String order = args.get("order") != null ? args.get("order").toString() : null;

        String dbType = metadataFacadeService.getDbType(connectionId);
        if (dbType == null) {
            return error("connection.notFound");
        }

        String queryFormat = queryFormatFor(dbType);

        Optional<QueryResult> qrOpt = metadataFacadeService.executeQuery(
                connectionId, catalog, namespace, entity, query, offset, effectiveLimit, sort, order);

        if (qrOpt.isEmpty()) {
            return error("error.queryExecutionFailed");
        }

        QueryResult qr = qrOpt.get();
        UnifiedQueryEnvelope envelope = UnifiedQueryEnvelope.fromQueryResult(
                qr, connectionId, dbType, queryFormat, catalog, namespace, entity, query);

        try {
            String json = objectMapper.writeValueAsString(envelope);
            return McpToolResponse.builder()
                    .content(java.util.List.of(McpToolContent.builder().type("text").text(json).build()))
                    .isError(false)
                    .build();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize query result", e);
            return error("Failed to serialize: " + e.getMessage());
        }
    }

    private static String queryFormatFor(String dbType) {
        return switch (dbType) {
            case "mongodb" -> "mql-json";
            case "cassandra" -> "cql";
            case "kafka" -> "json";
            case "redis" -> "pattern";
            case "elasticsearch" -> "json";
            default -> "sql";
        };
    }

    private McpToolResponse error(String message) {
        return McpToolResponse.builder()
                .isError(true)
                .error(message)
                .build();
    }

    private static Long toLong(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(o.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int toInt(Object o, int defaultValue) {
        if (o == null) {
            return defaultValue;
        }
        if (o instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(o.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
