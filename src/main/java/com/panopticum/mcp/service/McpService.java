package com.panopticum.mcp.service;

import com.panopticum.core.audit.AuditService;
import com.panopticum.i18n.Messages;
import com.panopticum.mcp.model.JsonRpcError;
import com.panopticum.mcp.model.JsonRpcRequest;
import com.panopticum.mcp.model.JsonRpcResponse;
import com.panopticum.mcp.model.McpToolRequest;
import com.panopticum.mcp.model.McpToolResponse;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class McpService {

    private static final int JSON_RPC_INVALID_REQUEST = -32600;
    private static final int JSON_RPC_METHOD_NOT_FOUND = -32601;
    private static final int JSON_RPC_INVALID_PARAMS = -32602;
    private static final int JSON_RPC_INTERNAL_ERROR = -32603;

    private final McpToolRegistry toolRegistry;
    private final AuditService auditService;

    public JsonRpcResponse handleRequest(JsonRpcRequest request) {
        log.debug("Received MCP request: {}", request);

        if (request == null) {
            return createErrorResponse(null, JSON_RPC_INVALID_REQUEST, "Invalid Request",
                    "Request body is required");
        }

        if (!"2.0".equals(request.getJsonrpc())) {
            return createErrorResponse(request.getId(), JSON_RPC_INVALID_REQUEST, "Invalid Request",
                    "jsonrpc must be '2.0'");
        }

        String method = request.getMethod();
        if (method == null) {
            return createErrorResponse(request.getId(), JSON_RPC_INVALID_REQUEST, "Invalid Request",
                    "method is required");
        }

        return switch (method) {
            case "initialize" -> handleInitialize(request);
            case "tools/list" -> handleToolsList(request);
            case "tools/call" -> handleToolsCall(request);
            default -> createErrorResponse(request.getId(), JSON_RPC_METHOD_NOT_FOUND, "Method not found",
                    "Unknown method: " + method);
        };
    }

    private JsonRpcResponse handleInitialize(JsonRpcRequest request) {
        auditService.mcpCall("initialize", null, null);

        Map<String, Object> result = new HashMap<>();
        Map<String, Object> capabilities = new HashMap<>();
        capabilities.put("tools", Map.of("listChanged", true));
        result.put("protocolVersion", "2025-06-18");
        result.put("capabilities", capabilities);
        result.put("serverInfo", Map.of("name", "Panopticum MCP Server", "version", "1.0.0"));

        return JsonRpcResponse.builder()
                .jsonrpc("2.0")
                .result(result)
                .id(request.getId())
                .build();
    }

    private JsonRpcResponse handleToolsList(JsonRpcRequest request) {
        auditService.mcpCall("tools/list", null, null);

        List<?> tools = toolRegistry.getAllTools();
        Map<String, Object> result = new HashMap<>();
        result.put("tools", tools);

        return JsonRpcResponse.builder()
                .jsonrpc("2.0")
                .result(result)
                .id(request.getId())
                .build();
    }

    @SuppressWarnings("unchecked")
    private JsonRpcResponse handleToolsCall(JsonRpcRequest request) {
        Map<String, Object> params = request.getParams();
        if (params == null) {
            return createErrorResponse(request.getId(), JSON_RPC_INVALID_PARAMS, "Invalid params",
                    "params is required");
        }

        String toolName = (String) params.get("name");
        if (toolName == null) {
            return createErrorResponse(request.getId(), JSON_RPC_INVALID_PARAMS, "Invalid params",
                    "name is required");
        }

        Map<String, Object> argumentsMap = (Map<String, Object>) params.get("arguments");
        Map<String, Object> arguments = argumentsMap != null ? argumentsMap : new HashMap<>();
        Long connectionId = parseConnectionId(arguments.get("connectionId"));
        auditService.mcpCall("tools/call", toolName, connectionId);

        McpToolRequest toolRequest = McpToolRequest.builder()
                .name(toolName)
                .arguments(arguments)
                .build();

        try {
            McpToolResponse mcpResponse = toolRegistry.executeTool(toolRequest);
            boolean isError = Boolean.TRUE.equals(mcpResponse.getIsError());

            Map<String, Object> result = new HashMap<>();
            if (mcpResponse.getContent() != null && !mcpResponse.getContent().isEmpty()) {
                result.put("content", mcpResponse.getContent());
            } else if (isError && mcpResponse.getError() != null) {
                result.put("content", List.of(Map.of("type", "text", "text", resolveMcpError(mcpResponse.getError()))));
            } else {
                result.put("content", List.of());
            }
            result.put("isError", isError);

            return JsonRpcResponse.builder()
                    .jsonrpc("2.0")
                    .result(result)
                    .id(request.getId())
                    .build();
        } catch (Exception e) {
            log.error("MCP tools/call failed tool={}", toolName, e);

            return createErrorResponse(request.getId(), JSON_RPC_INTERNAL_ERROR, "Internal error",
                    e.getMessage());
        }
    }

    private static String resolveMcpError(String message) {
        if (message == null || message.isBlank()) {
            return "error";
        }

        String resolved = Messages.forLocale("en").get(message);

        return resolved != null ? resolved : message;
    }

    private JsonRpcResponse createErrorResponse(Object id, int code, String message, String data) {
        return JsonRpcResponse.builder()
                .jsonrpc("2.0")
                .error(JsonRpcError.builder()
                        .code(code)
                        .message(message)
                        .data(data)
                        .build())
                .id(id)
                .build();
    }

    private static Long parseConnectionId(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number number) {
            return number.longValue();
        }

        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
