package com.panopticum.mcp.service;

import com.panopticum.mcp.model.McpToolRequest;
import com.panopticum.mcp.model.McpToolResponse;

public interface McpToolExecutor {

    String getToolName();

    McpToolResponse execute(McpToolRequest request);
}
