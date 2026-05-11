package com.panopticum.lightjdbc.controller;

import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.SqlQueryRequest;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.controller.AbstractConnectionApiController;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.ApiQueryParams;
import com.panopticum.lightjdbc.service.LightJdbcMetadataService;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
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

@Controller("/api/lightjdbc/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@Tag(name = "Light JDBC", description = "H2 / HSQLDB / Derby metadata and query API")
public class LightJdbcApiController extends AbstractConnectionApiController {

    private final LightJdbcMetadataService lightJdbcMetadataService;

    public LightJdbcApiController(DbConnectionService dbConnectionService,
                                LightJdbcMetadataService lightJdbcMetadataService) {
        super(dbConnectionService);
        this.lightJdbcMetadataService = lightJdbcMetadataService;
    }

    @Get("/{id}/catalogs")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List pseudo-catalog (connection database name)")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of catalogs"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<DatabaseInfo> catalogs(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return lightJdbcMetadataService.listDatabasesPaged(id, page, size, sort, order);
    }

    @Get("/{id}/schemas")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List schemas")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of schemas"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<SchemaInfo> schemas(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return lightJdbcMetadataService.listSchemasPaged(id, page, size, sort, order);
    }

    @Get("/{id}/schemas/{schema}/tables")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List tables in schema")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of tables"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<TableInfo> tables(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String schema,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return lightJdbcMetadataService.listTablesPaged(id, schema, page, size, sort, order);
    }

    @Post("/{id}/query")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute SQL")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Query result"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public QueryResult query(@Parameter(description = "Connection ID") @PathVariable Long id,
                             @Valid @Body SqlQueryRequest request) {
        ensureConnectionExists(id);
        if (request.getSql() == null || request.getSql().isBlank()) {
            return QueryResult.error("Empty query");
        }
        int offset = ApiQueryParams.normalizedOffset(request.getOffset());
        int limit = ApiQueryParams.normalizedLimit(request.getLimit());
        String search = ApiQueryParams.trimmedSearchOrEmpty(request.getSearch());
        return lightJdbcMetadataService.executeQuery(id, request.getSql(), offset, limit,
                request.getSort() != null ? request.getSort() : "",
                request.getOrder() != null ? request.getOrder() : "",
                search).orElse(QueryResult.error("error.queryExecutionFailed"));
    }
}
