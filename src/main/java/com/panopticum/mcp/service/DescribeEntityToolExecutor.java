package com.panopticum.mcp.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.mcp.model.EntityDescription;
import com.panopticum.mcp.model.McpToolContent;
import com.panopticum.mcp.model.McpToolRequest;
import com.panopticum.mcp.model.McpToolResponse;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class DescribeEntityToolExecutor implements McpToolExecutor {

    private final MetadataFacadeService metadataFacadeService;
    private final ObjectMapper objectMapper;

    @Override
    public String getToolName() {
        return "describe-entity";
    }

    @Override
    public McpToolResponse execute(McpToolRequest request) {
        Map<String, Object> args = request.getArguments() != null ? request.getArguments() : Map.of();
        Long connectionId = toLong(args.get("connectionId"));
        if (connectionId == null) {
            return error("connectionId (number) is required");
        }

        String entity = args.get("entity") != null ? args.get("entity").toString() : null;
        if (entity == null || entity.isBlank()) {
            return error("entity (string) is required");
        }

        String catalog = args.get("catalog") != null ? args.get("catalog").toString() : null;
        String namespace = args.get("namespace") != null ? args.get("namespace").toString() : null;
        int sampleSize = toInt(args.get("sampleSize"), 100);

        String dbType = metadataFacadeService.getDbType(connectionId);
        if (dbType == null) {
            return error("connection.notFound");
        }

        log.info("MCP describe-entity connectionId={} entity={}", connectionId, entity);

        Optional<EntityDescription> descOpt = metadataFacadeService.describeEntity(
                connectionId, catalog, namespace, entity, sampleSize);

        if (descOpt.isEmpty()) {
            return error("describe.notSupported");
        }

        try {
            String json = objectMapper.writeValueAsString(descOpt.get());
            return McpToolResponse.builder()
                    .content(List.of(McpToolContent.builder().type("text").text(json).build()))
                    .isError(false)
                    .build();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize entity description", e);
            return error("Failed to serialize: " + e.getMessage());
        }
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
