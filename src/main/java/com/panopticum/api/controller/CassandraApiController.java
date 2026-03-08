package com.panopticum.api.controller;

import com.panopticum.api.model.CassandraQueryRequest;
import com.panopticum.cassandra.model.CassandraKeyspaceInfo;
import com.panopticum.cassandra.model.CassandraTableInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.cassandra.service.CassandraMetadataService;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.MediaType;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@Controller("/api/cassandra/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Tag(name = "Cassandra", description = "Cassandra keyspaces, tables and CQL query API")
public class CassandraApiController {

    private final DbConnectionService dbConnectionService;
    private final CassandraMetadataService cassandraMetadataService;

    @Get("/{id}/keyspaces")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List keyspaces")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Keyspaces page"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<CassandraKeyspaceInfo> keyspaces(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return cassandraMetadataService.listKeyspacesPaged(id, page, size, sort, order);
    }

    @Get("/{id}/keyspaces/{keyspaceName}/tables")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List tables")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Tables page"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<CassandraTableInfo> tables(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String keyspaceName,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return cassandraMetadataService.listTablesPaged(id, keyspaceName, page, size, sort, order);
    }

    @Post("/{id}/keyspaces/{keyspaceName}/query")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute CQL query")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Query result"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public QueryResult query(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String keyspaceName,
            @Valid @Body CassandraQueryRequest request) {
        ensureConnectionExists(id);
        String cql = request.getCql();
        if (cql == null || cql.isBlank()) {
            return QueryResult.error("Empty query");
        }
        int offset = Math.max(0, request.getOffset());
        int limit = request.getLimit() > 0 ? Math.min(request.getLimit(), 1000) : 100;
        return cassandraMetadataService.executeQuery(id, keyspaceName, cql, offset, limit)
                .orElse(QueryResult.error("error.queryExecutionFailed"));
    }

    private void ensureConnectionExists(Long id) {
        if (dbConnectionService.findById(id).isEmpty()) {
            throw new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound");
        }
    }
}
