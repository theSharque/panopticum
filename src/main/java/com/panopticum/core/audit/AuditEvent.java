package com.panopticum.core.audit;

import lombok.Builder;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;

@Value
@Builder
public class AuditEvent {

    AuditAction action;
    String user;
    Long connectionId;
    String connectionType;
    String scope;
    String kind;
    String name;
    String method;
    String route;
    Integer status;
    Long durationMs;
    String mcpMethod;
    String tool;

    public String format() {
        List<String> parts = new ArrayList<>();
        parts.add("AUDIT");
        append(parts, "user", user);
        append(parts, "conn", connectionId != null ? String.valueOf(connectionId) : null);
        parts.add("action=" + action.name());
        append(parts, "type", connectionType);
        append(parts, "scope", scope);
        append(parts, "kind", kind);
        append(parts, "name", name);
        append(parts, "method", method);
        append(parts, "route", route);
        append(parts, "status", status != null ? String.valueOf(status) : null);
        append(parts, "durationMs", durationMs != null ? String.valueOf(durationMs) : null);
        append(parts, "mcpMethod", mcpMethod);
        append(parts, "tool", tool);
        return String.join(" ", parts);
    }

    private static void append(List<String> parts, String key, String value) {
        if (value != null && !value.isBlank()) {
            parts.add(key + "=" + value);
        }
    }
}
