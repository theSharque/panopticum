package com.panopticum.mcp.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.mcp.model.McpToolContent;
import com.panopticum.mcp.model.McpToolRequest;
import com.panopticum.mcp.model.McpToolResponse;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class ResolvePanopticumLinkToolExecutor implements McpToolExecutor {

    private final PanopticumLinkResolverService linkResolverService;
    private final ObjectMapper objectMapper;

    @Override
    public String getToolName() {
        return "resolve-panopticum-link";
    }

    @Override
    public McpToolResponse execute(McpToolRequest request) {
        Map<String, Object> args = request.getArguments() != null ? request.getArguments() : Map.of();
        Object linkArg = args.get("link");
        if (linkArg == null || linkArg.toString().isBlank()) {
            return errorResponse("link (string) is required", List.of());
        }

        PanopticumLinkResolverService.ResolveOutcome outcome = linkResolverService.resolve(linkArg.toString());
        if (outcome.success()) {
            return jsonResponse(outcome.data(), false);
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", outcome.error());
        body.put("availablePaths", outcome.availablePaths());
        return jsonResponse(body, true);
    }

    private McpToolResponse jsonResponse(Map<String, Object> body, boolean isError) {
        try {
            String json = objectMapper.writeValueAsString(body);
            return McpToolResponse.builder()
                    .content(List.of(McpToolContent.builder().type("text").text(json).build()))
                    .isError(isError)
                    .build();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize resolve-panopticum-link result", e);
            return McpToolResponse.builder()
                    .isError(true)
                    .error("Failed to serialize result: " + e.getMessage())
                    .build();
        }
    }

    private McpToolResponse errorResponse(String message, List<String> availablePaths) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", message);
        body.put("availablePaths", availablePaths);
        return jsonResponse(body, true);
    }
}
