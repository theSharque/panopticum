package com.panopticum.api.controller;

import com.panopticum.api.model.ConnectionRequest;
import com.panopticum.api.model.ConnectionTestRequest;
import com.panopticum.api.model.ConnectionTestResponse;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.ConnectionTestService;
import com.panopticum.core.service.DbConnectionFactory;
import com.panopticum.core.service.DbConnectionService;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.Status;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.MediaType;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

@Controller("/api/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Tag(name = "Connections", description = "Connection CRUD and test")
public class ConnectionsApiController {

    private final DbConnectionService dbConnectionService;
    private final DbConnectionFactory dbConnectionFactory;
    private final ConnectionTestService connectionTestService;

    @Value("${panopticum.admin-lock:false}")
    private boolean adminLock;

    @Get
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List all connections")
    @ApiResponse(responseCode = "200", description = "List of connections")
    public List<DbConnection> list() {
        return dbConnectionService.findAll();
    }

    @Get("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get connection by ID")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Connection found"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public DbConnection get(@Parameter(description = "Connection ID") @PathVariable Long id) {
        return dbConnectionService.findById(id)
                .orElseThrow(() -> new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound"));
    }

    @Post
    @Produces(MediaType.APPLICATION_JSON)
    @Status(HttpStatus.OK)
    @Operation(summary = "Create or update connection")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Connection saved"),
            @ApiResponse(responseCode = "400", description = "Bad request"),
            @ApiResponse(responseCode = "403", description = "admin.lock.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public DbConnection save(@Valid @Body ConnectionRequest request) {
        assertNotLocked();
        String type = normalizeType(request.getType());
        Long id = request.getId();
        String username = "redis".equalsIgnoreCase(type) ? null : request.getUsername();
        if (id != null) {
            DbConnection existing = dbConnectionService.findById(id)
                    .orElseThrow(() -> new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound"));
            if (!type.equalsIgnoreCase(existing.getType())) {
                throw new HttpStatusException(HttpStatus.BAD_REQUEST, "connection.typeMismatch");
            }
            String pwd = (request.getPassword() != null && !request.getPassword().isBlank())
                    ? request.getPassword() : existing.getPassword();
            DbConnection conn = dbConnectionFactory.build(type, request.getName(), request.getHost(),
                    request.getPort(), request.getDatabase(), username, pwd);
            conn.setId(id);
            return dbConnectionService.save(conn);
        }
        DbConnection conn = dbConnectionFactory.build(type, request.getName(), request.getHost(),
                request.getPort(), request.getDatabase(), username, request.getPassword());
        return dbConnectionService.save(conn);
    }

    @Post("/test")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Test connection")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Test result"),
            @ApiResponse(responseCode = "404", description = "connection.notFound when id provided and not found")
    })
    public ConnectionTestResponse test(@Valid @Body ConnectionTestRequest request) {
        String type = normalizeType(request.getType());
        String host = request.getHost() != null ? request.getHost() : "";
        Integer port = request.getPort();
        String database = request.getDatabase() != null ? request.getDatabase() : "";
        String username = request.getUsername() != null ? request.getUsername() : "";
        String password = resolvePasswordForTest(request.getId(), request.getPassword());
        try {
            Optional<String> error = connectionTestService.test(type, host, port, database, username, password);
            boolean success = error.isEmpty();
            String messageKey = error.orElse("connectionTest.success");
            return new ConnectionTestResponse(success, messageKey);
        } catch (Exception e) {
            String messageKey = e.getMessage() != null ? e.getMessage() : "error.queryExecutionFailed";
            return new ConnectionTestResponse(false, messageKey);
        }
    }

    @Delete("/{id}")
    @Status(HttpStatus.NO_CONTENT)
    @Operation(summary = "Delete connection")
    @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Deleted"),
            @ApiResponse(responseCode = "403", description = "admin.lock.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public void delete(@Parameter(description = "Connection ID") @PathVariable Long id) {
        assertNotLocked();
        if (dbConnectionService.findById(id).isEmpty()) {
            throw new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound");
        }
        dbConnectionService.deleteById(id);
    }

    private void assertNotLocked() {
        if (adminLock) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "admin.lock.enabled");
        }
    }

    private String resolvePasswordForTest(Long id, String password) {
        if (password != null && !password.isBlank()) {
            return password;
        }
        if (id != null) {
            return dbConnectionService.findById(id)
                    .map(DbConnection::getPassword)
                    .orElse("");
        }
        return password != null ? password : "";
    }

    private static String normalizeType(String type) {
        if (type == null || type.isBlank()) {
            return type;
        }
        return "mssql".equalsIgnoreCase(type) ? "sqlserver" : type;
    }
}
