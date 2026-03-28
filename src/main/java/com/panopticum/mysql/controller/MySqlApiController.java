package com.panopticum.mysql.controller;

import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SqlQueryRequest;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.controller.AbstractConnectionApiController;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.ApiQueryParams;
import com.panopticum.core.model.JdbcRowDetailResponse;
import com.panopticum.core.model.JdbcRowUpdateRequest;
import com.panopticum.mysql.service.MySqlMetadataService;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Put;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/api/mysql/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@Tag(name = "MySQL", description = "MySQL metadata and query API")
public class MySqlApiController extends AbstractConnectionApiController {

    private final MySqlMetadataService mySqlMetadataService;

    public MySqlApiController(DbConnectionService dbConnectionService, MySqlMetadataService mySqlMetadataService) {
        super(dbConnectionService);
        this.mySqlMetadataService = mySqlMetadataService;
    }

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
        return mySqlMetadataService.listDatabasesPaged(id, page, size, sort, order);
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
        return mySqlMetadataService.listTablesPaged(id, dbName, page, size, sort, order);
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
            @Valid @Body SqlQueryRequest request) {
        ensureConnectionExists(id);
        if (request.getSql() == null || request.getSql().isBlank()) {
            return QueryResult.error("Empty query");
        }
        int offset = ApiQueryParams.normalizedOffset(request.getOffset());
        int limit = ApiQueryParams.normalizedLimit(request.getLimit());
        String search = ApiQueryParams.trimmedSearchOrEmpty(request.getSearch());
        return mySqlMetadataService.executeQuery(id, request.getDbName(), request.getSql(), offset, limit,
                request.getSort() != null ? request.getSort() : "",
                request.getOrder() != null ? request.getOrder() : "",
                search).orElse(QueryResult.error("error.queryExecutionFailed"));
    }

    @Get("/{id}/databases/{dbName}/row/detail")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get row detail by index")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Row detail"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public JdbcRowDetailResponse rowDetail(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @QueryValue String sql,
            @QueryValue(value = "rowNum", defaultValue = "0") int rowNum,
            @QueryValue(defaultValue = "") String sort,
            @QueryValue(defaultValue = "") String order) {
        ensureConnectionExists(id);
        Map<String, Object> result = mySqlMetadataService.getDetailRow(id, dbName, sql != null ? sql : "",
                Math.max(0, rowNum), sort != null ? sort : "", order != null ? order : "");
        return JdbcRowDetailResponse.fromEditableRowMap(result);
    }

    @Put("/{id}/databases/{dbName}/row")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Update row by primary key")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Updated row detail"),
            @ApiResponse(responseCode = "403", description = "read.only.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public JdbcRowDetailResponse updateRow(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @Valid @Body JdbcRowUpdateRequest request) {
        assertNotReadOnly();
        ensureConnectionExists(id);
        Map<String, String> columnValues = request.getColumnValues() != null ? request.getColumnValues() : Map.of();
        Map<String, Object> keyValues = new LinkedHashMap<>();
        List<String> uniqueKeyColumns = request.getUniqueKeyColumns() != null ? request.getUniqueKeyColumns() : List.of();
        for (String col : uniqueKeyColumns) {
            String val = columnValues.get(col);
            keyValues.put(col, val != null && !val.isBlank() ? val : null);
        }
        Map<String, String> updateColumns = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : columnValues.entrySet()) {
            if (!uniqueKeyColumns.contains(e.getKey())) {
                updateColumns.put(e.getKey(), e.getValue() != null ? e.getValue() : "");
            }
        }
        Optional<String> err = mySqlMetadataService.executeUpdateByKey(id, dbName, request.getQualifiedTable(),
                uniqueKeyColumns, keyValues, updateColumns);
        if (err.isPresent()) {
            return new JdbcRowDetailResponse(err.get(), List.of(), false, uniqueKeyColumns, request.getQualifiedTable());
        }
        Map<String, Object> result = mySqlMetadataService.getDetailRow(id, dbName, request.getSql(),
                request.getRowNum(), request.getSort() != null ? request.getSort() : "",
                request.getOrder() != null ? request.getOrder() : "");
        return JdbcRowDetailResponse.fromEditableRowMap(result);
    }
}
