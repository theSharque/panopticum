package com.panopticum.mcp.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Serdeable
public class JsonRpcResponse {

    private String jsonrpc;
    private Object result;
    private JsonRpcError error;
    private Object id;
}
