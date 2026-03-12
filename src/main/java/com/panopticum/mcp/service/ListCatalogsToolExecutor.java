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
public class ListCatalogsToolExecutor implements McpToolExecutor {

    private final MetadataFacadeService metadataFacadeService;
    private final ObjectMapper objectMapper;

    @Override
    public String getToolName() {
        return "list-catalogs";
    }

    @Override
    public McpToolResponse execute(McpToolRequest request) {
        Map<String, Object> args = request.getArguments() != null ? request.getArguments() : Map.of();
        Long connectionId = toLong(args.get("connectionId"));
        if (connectionId == null) {
            return error("connectionId (number) is required");
        }

        int page = toInt(args.get("page"), 1);
        int size = Math.min(Math.max(1, toInt(args.get("size"), 50)), 100);
        String sort = args.get("sort") != null ? args.get("sort").toString() : "name";
        String order = args.get("order") != null ? args.get("order").toString() : "asc";

        Map<String, Object> result = metadataFacadeService.listCatalogs(connectionId, page, size, sort, order);
        if (result.containsKey("error")) {
            return error(result.get("error").toString());
        }

        return toJsonResponse(result);
    }

    private McpToolResponse error(String message) {
        return McpToolResponse.builder()
                .isError(true)
                .error(message)
                .build();
    }

    private McpToolResponse toJsonResponse(Object result) {
        try {
            String json = objectMapper.writeValueAsString(result);
            return McpToolResponse.builder()
                    .content(java.util.List.of(McpToolContent.builder().type("text").text(json).build()))
                    .isError(false)
                    .build();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize result", e);
            return error("Failed to serialize: " + e.getMessage());
        }
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
