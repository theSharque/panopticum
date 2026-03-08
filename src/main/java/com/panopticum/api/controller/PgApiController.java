package com.panopticum.api.controller;

import com.panopticum.api.model.PgQueryRequest;
import com.panopticum.api.model.PgRowDetailResponse;
import com.panopticum.api.model.PgRowUpdateRequest;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.postgres.service.PgMetadataService;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Nullable;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/api/pg/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Tag(name = "PostgreSQL", description = "PostgreSQL metadata and query API")
public class PgApiController {

    private final DbConnectionService dbConnectionService;
    private final PgMetadataService pgMetadataService;

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
        return pgMetadataService.listDatabasesPaged(id, page, size, sort, order);
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
        return pgMetadataService.listSchemasPaged(id, dbName, page, size, sort, order);
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
        String schemaClean = unquotePgIdentifier(schema);
        return pgMetadataService.listTablesPaged(id, dbName, schemaClean, page, size, sort, order);
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
        return pgMetadataService.executeQuery(id, request.getDbName(), request.getSql(), off, lim,
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
    public PgRowDetailResponse rowDetail(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @PathVariable String schema,
            @QueryValue String sql,
            @QueryValue(value = "rowNum", defaultValue = "0") int rowNum,
            @QueryValue(defaultValue = "") String sort,
            @QueryValue(defaultValue = "") String order,
            @QueryValue @Nullable String search) {
        ensureConnectionExists(id);
        String schemaClean = unquotePgIdentifier(schema);
        Map<String, Object> result = pgMetadataService.getDetailRowWithCtid(id, dbName, schemaClean,
                sql != null ? sql : "", Math.max(0, rowNum),
                sort != null ? sort : "", order != null ? order : "");
        String error = (String) result.get("error");
        @SuppressWarnings("unchecked")
        List<Map<String, String>> detailRows = (List<Map<String, String>>) result.get("detailRows");
        String rowCtid = (String) result.get("rowCtid");
        return new PgRowDetailResponse(error, detailRows != null ? detailRows : List.of(),
                rowCtid != null ? rowCtid : "");
    }

    @Put("/{id}/databases/{dbName}/schemas/{schema}/row")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Update row by CTID")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Updated row detail"),
            @ApiResponse(responseCode = "403", description = "read.only.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public PgRowDetailResponse updateRow(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @PathVariable String schema,
            @Valid @Body PgRowUpdateRequest request) {
        assertNotReadOnly();
        ensureConnectionExists(id);
        String schemaClean = unquotePgIdentifier(schema);
        Optional<String> qualifiedTable = pgMetadataService.parseTableFromSql(request.getSql());
        if (qualifiedTable.isEmpty()) {
            return new PgRowDetailResponse("Could not determine table from SQL.", List.of(), "");
        }
        Map<String, String> columnValues = request.getColumnValues() != null ? request.getColumnValues() : Map.of();
        Optional<String> err = pgMetadataService.executeUpdateByCtid(id, dbName, qualifiedTable.get(),
                request.getCtid(), columnValues);
        if (err.isPresent()) {
            return new PgRowDetailResponse(err.get(), List.of(), "");
        }
        Map<String, Object> result = pgMetadataService.getDetailRowWithCtid(id, dbName, schemaClean,
                request.getSql(), request.getRowNum(),
                request.getSort() != null ? request.getSort() : "",
                request.getOrder() != null ? request.getOrder() : "");
        String error = (String) result.get("error");
        @SuppressWarnings("unchecked")
        List<Map<String, String>> detailRows = (List<Map<String, String>>) result.get("detailRows");
        String rowCtid = (String) result.get("rowCtid");
        return new PgRowDetailResponse(error, detailRows != null ? detailRows : List.of(),
                rowCtid != null ? rowCtid : "");
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

    private static String unquotePgIdentifier(String s) {
        if (s == null || s.isBlank()) {
            return s != null ? s : "";
        }
        String t = s.trim();
        if (!t.isEmpty() && t.charAt(0) == '"') {
            t = t.substring(1);
        }
        if (!t.isEmpty() && t.charAt(t.length() - 1) == '"') {
            t = t.substring(0, t.length() - 1);
        }
        return t.replace("\"\"", "\"");
    }
}
