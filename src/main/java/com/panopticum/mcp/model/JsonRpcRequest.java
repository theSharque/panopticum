package com.panopticum.mcp.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Serdeable
public class JsonRpcRequest {

    private String jsonrpc;
    private String method;
    private Map<String, Object> params;
    private Object id;
}
