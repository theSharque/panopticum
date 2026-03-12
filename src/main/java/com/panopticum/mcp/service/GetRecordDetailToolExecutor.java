package com.panopticum.mcp.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.mcp.model.McpToolContent;
import com.panopticum.mcp.model.McpToolRequest;
import com.panopticum.mcp.model.McpToolResponse;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class GetRecordDetailToolExecutor implements McpToolExecutor {

    private final MetadataFacadeService metadataFacadeService;
    private final ObjectMapper objectMapper;

    @Override
    public String getToolName() {
        return "get-record-detail";
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
        String documentId = args.get("documentId") != null ? args.get("documentId").toString() : null;
        @SuppressWarnings("unchecked")
        Map<String, Object> primaryKey = args.get("primaryKey") instanceof Map ? (Map<String, Object>) args.get("primaryKey") : null;
        String locator = args.get("locator") != null ? args.get("locator").toString() : null;

        if ((documentId == null || documentId.isBlank())
                && (primaryKey == null || primaryKey.isEmpty())
                && (locator == null || locator.isBlank())) {
            return error("One of documentId (Mongo), primaryKey (object), or locator (engine-specific) is required");
        }

        java.util.Optional<Map<String, Object>> detailOpt = metadataFacadeService.getRecordDetail(
                connectionId, catalog, namespace, entity, documentId, primaryKey, locator);

        if (detailOpt.isEmpty()) {
            return error("Record not found or get-record-detail not supported for this connection type");
        }

        try {
            String json = objectMapper.writeValueAsString(detailOpt.get());
            return McpToolResponse.builder()
                    .content(java.util.List.of(McpToolContent.builder().type("text").text(json).build()))
                    .isError(false)
                    .build();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize record detail", e);
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
}
