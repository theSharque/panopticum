package com.panopticum.mssql.controller;

import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.SqlQueryRequest;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.mssql.service.MssqlMetadataService;
import com.panopticum.mysql.model.MySqlRowDetailResponse;
import com.panopticum.mysql.model.MySqlRowUpdateRequest;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Put;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/api/mssql/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Tag(name = "MSSQL", description = "Microsoft SQL Server metadata and query API")
public class MssqlApiController {

    private final DbConnectionService dbConnectionService;
    private final MssqlMetadataService mssqlMetadataService;

    @Value("${panopticum.read-only:false}")
    private boolean readOnly;

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
        return mssqlMetadataService.listDatabasesPaged(id, page, size, sort, order);
    }

    @Get("/{id}/databases/{dbName}/schemas")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List schemas")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of schemas"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<SchemaInfo> schemas(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return mssqlMetadataService.listSchemasPaged(id, dbName, page, size, sort, order);
    }

    @Get("/{id}/databases/{dbName}/schemas/{schema}/tables")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List tables")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of tables"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<TableInfo> tables(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @PathVariable String schema,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return mssqlMetadataService.listTablesPaged(id, dbName, schema, page, size, sort, order);
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
        int off = request.getOffset() != null ? Math.max(0, request.getOffset()) : 0;
        int lim = request.getLimit() != null && request.getLimit() > 0 ? Math.min(request.getLimit(), 1000) : 100;
        String search = request.getSearch() != null && !request.getSearch().isBlank() ? request.getSearch().trim() : "";
        return mssqlMetadataService.executeQuery(id, request.getDbName(), request.getSql(), off, lim,
                request.getSort() != null ? request.getSort() : "",
                request.getOrder() != null ? request.getOrder() : "",
                search).orElse(QueryResult.error("error.queryExecutionFailed"));
    }

    @Get("/{id}/databases/{dbName}/schemas/{schema}/row/detail")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get row detail by index")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Row detail"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public MySqlRowDetailResponse rowDetail(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @PathVariable String schema,
            @QueryValue String sql,
            @QueryValue(value = "rowNum", defaultValue = "0") int rowNum,
            @QueryValue(defaultValue = "") String sort,
            @QueryValue(defaultValue = "") String order) {
        ensureConnectionExists(id);
        Map<String, Object> result = mssqlMetadataService.getDetailRow(id, dbName, schema, sql != null ? sql : "",
                Math.max(0, rowNum), sort != null ? sort : "", order != null ? order : "");
        return toRowDetailResponse(result);
    }

    @Put("/{id}/databases/{dbName}/schemas/{schema}/row")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Update row by primary key")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Updated row detail"),
            @ApiResponse(responseCode = "403", description = "read.only.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public MySqlRowDetailResponse updateRow(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @PathVariable String schema,
            @Valid @Body MySqlRowUpdateRequest request) {
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
        Optional<String> err = mssqlMetadataService.executeUpdateByKey(id, dbName, schema, request.getQualifiedTable(),
                uniqueKeyColumns, keyValues, updateColumns);
        if (err.isPresent()) {
            return new MySqlRowDetailResponse(err.get(), List.of(), false, uniqueKeyColumns, request.getQualifiedTable());
        }
        Map<String, Object> result = mssqlMetadataService.getDetailRow(id, dbName, schema, request.getSql(),
                request.getRowNum(), request.getSort() != null ? request.getSort() : "",
                request.getOrder() != null ? request.getOrder() : "");
        return toRowDetailResponse(result);
    }

    private MySqlRowDetailResponse toRowDetailResponse(Map<String, Object> result) {
        String error = (String) result.get("error");
        @SuppressWarnings("unchecked")
        List<Map<String, String>> detailRows = (List<Map<String, String>>) result.get("detailRows");
        boolean editable = Boolean.TRUE.equals(result.get("editable"));
        @SuppressWarnings("unchecked")
        List<String> uniqueKeyColumns = (List<String>) result.get("uniqueKeyColumns");
        String qualifiedTable = (String) result.get("qualifiedTable");
        return new MySqlRowDetailResponse(error, detailRows != null ? detailRows : List.of(),
                editable, uniqueKeyColumns != null ? uniqueKeyColumns : List.of(),
                qualifiedTable != null ? qualifiedTable : "");
    }

    private void ensureConnectionExists(Long id) {
        if (dbConnectionService.findById(id).isEmpty()) {
            throw new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound");
        }
    }

    private void assertNotReadOnly() {
        if (readOnly) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "read.only.enabled");
        }
    }
}
