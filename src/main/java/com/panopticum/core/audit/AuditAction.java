package com.panopticum.core.audit;

public enum AuditAction {
    OPEN_DATABASE,
    OPEN_SCHEMA,
    OPEN_TABLE,
    RUN_QUERY,
    ROW_UPDATE,
    CONNECTION_CREATE,
    CONNECTION_UPDATE,
    CONNECTION_DELETE,
    API_CALL,
    MCP_CALL
}
