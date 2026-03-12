package com.panopticum.mcp.controller;

import com.panopticum.mcp.model.JsonRpcRequest;
import com.panopticum.mcp.model.JsonRpcResponse;
import com.panopticum.mcp.service.McpService;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import lombok.RequiredArgsConstructor;

@Controller("/mcp")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor_ = @Inject)
@Tag(name = "MCP", description = "Model Context Protocol JSON-RPC endpoint")
public class McpController {

    private final McpService mcpService;

    @Post
    @Operation(summary = "Handle MCP JSON-RPC request", description = "Accepts initialize, tools/list, tools/call. Requires Basic Auth.")
    public JsonRpcResponse handleRequest(@Body JsonRpcRequest request) {
        return mcpService.handleRequest(request);
    }
}
