package com.panopticum.api.controller;

import com.panopticum.api.model.PgQueryRequest;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.clickhouse.service.ClickHouseMetadataService;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.annotation.Produces;
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

@Controller("/api/clickhouse/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Tag(name = "ClickHouse", description = "ClickHouse metadata and query API")
public class ClickHouseApiController {

    private final DbConnectionService dbConnectionService;
    private final ClickHouseMetadataService clickHouseMetadataService;

    @Get("/{id}/databases")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List databases")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of databases"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<DatabaseInfo> databases(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return clickHouseMetadataService.listDatabasesPaged(id, page, size, sort, order);
    }

    @Get("/{id}/databases/{dbName}/tables")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List tables")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of tables"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<TableInfo> tables(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return clickHouseMetadataService.listTablesPaged(id, dbName, page, size, sort, order);
    }

    @Post("/{id}/query")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute SQL query")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Query result"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public QueryResult query(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @Valid @Body PgQueryRequest request) {
        ensureConnectionExists(id);
        if (request.getSql() == null || request.getSql().isBlank()) {
            return QueryResult.error("Empty query");
        }
        int off = request.getOffset() != null ? Math.max(0, request.getOffset()) : 0;
        int lim = request.getLimit() != null && request.getLimit() > 0 ? Math.min(request.getLimit(), 1000) : 100;
        String search = request.getSearch() != null && !request.getSearch().isBlank() ? request.getSearch().trim() : "";
        return clickHouseMetadataService.executeQuery(id, request.getDbName(), request.getSql(), off, lim,
                request.getSort() != null ? request.getSort() : "",
                request.getOrder() != null ? request.getOrder() : "",
                search).orElse(QueryResult.error("error.queryExecutionFailed"));
    }

    private void ensureConnectionExists(Long id) {
        if (dbConnectionService.findById(id).isEmpty()) {
            throw new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound");
        }
    }
}
