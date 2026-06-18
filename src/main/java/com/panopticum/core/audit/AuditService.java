package com.panopticum.core.audit;

import com.panopticum.core.sql.SqlStatementKind;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@RequiredArgsConstructor
public class AuditService {

    private static final Logger AUDIT_LOG = LoggerFactory.getLogger("com.panopticum.audit");

    private final AuditUserResolver userResolver;

    @Value("${panopticum.audit.enabled:true}")
    private boolean enabled;

    public void record(AuditEvent event) {
        if (!enabled) {
            return;
        }

        AUDIT_LOG.info(event.format());
    }

    public void browse(Long connectionId, String connectionType, AuditAction action, String scope) {
        record(AuditEvent.builder()
                .action(action)
                .user(userResolver.currentUser())
                .connectionId(connectionId)
                .connectionType(connectionType)
                .scope(scope)
                .build());
    }

    public void query(Long connectionId, String connectionType, SqlStatementKind kind) {
        record(AuditEvent.builder()
                .action(AuditAction.RUN_QUERY)
                .user(userResolver.currentUser())
                .connectionId(connectionId)
                .connectionType(connectionType)
                .kind(kind != null ? kind.name() : null)
                .build());
    }

    public void rowUpdate(Long connectionId, String connectionType) {
        record(AuditEvent.builder()
                .action(AuditAction.ROW_UPDATE)
                .user(userResolver.currentUser())
                .connectionId(connectionId)
                .connectionType(connectionType)
                .build());
    }

    public void connectionCreate(Long connectionId, String connectionType, String name) {
        record(AuditEvent.builder()
                .action(AuditAction.CONNECTION_CREATE)
                .user(userResolver.currentUser())
                .connectionId(connectionId)
                .connectionType(connectionType)
                .name(name)
                .build());
    }

    public void connectionUpdate(Long connectionId, String connectionType, String name) {
        record(AuditEvent.builder()
                .action(AuditAction.CONNECTION_UPDATE)
                .user(userResolver.currentUser())
                .connectionId(connectionId)
                .connectionType(connectionType)
                .name(name)
                .build());
    }

    public void connectionDelete(Long connectionId, String connectionType, String name) {
        record(AuditEvent.builder()
                .action(AuditAction.CONNECTION_DELETE)
                .user(userResolver.currentUser())
                .connectionId(connectionId)
                .connectionType(connectionType)
                .name(name)
                .build());
    }

    public void apiCall(String method, String route, int status, long durationMs) {
        record(AuditEvent.builder()
                .action(AuditAction.API_CALL)
                .user(userResolver.currentUser())
                .method(method)
                .route(route)
                .status(status)
                .durationMs(durationMs)
                .build());
    }

    public void mcpCall(String mcpMethod, String tool, Long connectionId) {
        record(AuditEvent.builder()
                .action(AuditAction.MCP_CALL)
                .user(userResolver.currentUser())
                .mcpMethod(mcpMethod)
                .tool(tool)
                .connectionId(connectionId)
                .build());
    }
}
