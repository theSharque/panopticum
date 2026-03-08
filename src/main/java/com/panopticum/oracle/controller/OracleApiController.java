package com.panopticum.oracle.controller;

import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.oracle.model.OracleQueryRequest;
import com.panopticum.oracle.model.OracleRowDetailResponse;
import com.panopticum.oracle.model.OracleRowUpdateRequest;
import com.panopticum.oracle.service.OracleMetadataService;
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

@Controller("/api/oracle/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Tag(name = "Oracle", description = "Oracle metadata and query API")
public class OracleApiController {

    private final DbConnectionService dbConnectionService;
    private final OracleMetadataService oracleMetadataService;

    @Value("${panopticum.read-only:false}")
    private boolean readOnly;

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
        return oracleMetadataService.listSchemasPaged(id, page, size, sort, order);
    }

    @Get("/{id}/schemas/{schema}/tables")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List tables")
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
        String schemaClean = unquote(schema);
        return oracleMetadataService.listTablesPaged(id, schemaClean, page, size, sort, order);
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
            @Valid @Body OracleQueryRequest request) {
        ensureConnectionExists(id);
        if (request.getSql() == null || request.getSql().isBlank()) {
            return QueryResult.error("Empty query");
        }
        int off = request.getOffset() != null ? Math.max(0, request.getOffset()) : 0;
        int lim = request.getLimit() != null && request.getLimit() > 0 ? Math.min(request.getLimit(), 1000) : 100;
        String search = request.getSearch() != null && !request.getSearch().isBlank() ? request.getSearch().trim() : "";
        return oracleMetadataService.executeQuery(id, request.getSchema(), request.getSql(), off, lim,
                request.getSort() != null ? request.getSort() : "",
                request.getOrder() != null ? request.getOrder() : "",
                search).orElse(QueryResult.error("error.queryExecutionFailed"));
    }

    @Get("/{id}/schemas/{schema}/row/detail")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get row detail by index")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Row detail"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public OracleRowDetailResponse rowDetail(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String schema,
            @QueryValue String sql,
            @QueryValue(value = "rowNum", defaultValue = "0") int rowNum,
            @QueryValue(defaultValue = "") String sort,
            @QueryValue(defaultValue = "") String order) {
        ensureConnectionExists(id);
        String schemaClean = unquote(schema);
        Map<String, Object> result = oracleMetadataService.getDetailRowWithRowid(id, schemaClean, sql != null ? sql : "",
                Math.max(0, rowNum), sort != null ? sort : "", order != null ? order : "");
        String error = (String) result.get("error");
        @SuppressWarnings("unchecked")
        List<Map<String, String>> detailRows = (List<Map<String, String>>) result.get("detailRows");
        String rowRowid = (String) result.get("rowRowid");
        return new OracleRowDetailResponse(error, detailRows != null ? detailRows : List.of(),
                rowRowid != null ? rowRowid : "");
    }

    @Put("/{id}/schemas/{schema}/row")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Update row by ROWID")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Updated row detail"),
            @ApiResponse(responseCode = "403", description = "read.only.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public OracleRowDetailResponse updateRow(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String schema,
            @Valid @Body OracleRowUpdateRequest request) {
        assertNotReadOnly();
        ensureConnectionExists(id);
        String schemaClean = unquote(schema);
        Optional<String> qualifiedTable = oracleMetadataService.parseTableFromSql(request.getSql());
        if (qualifiedTable.isEmpty()) {
            return new OracleRowDetailResponse("Could not determine table from SQL.", List.of(), "");
        }
        Map<String, String> columnValues = request.getColumnValues() != null ? request.getColumnValues() : Map.of();
        Optional<String> err = oracleMetadataService.executeUpdateByRowid(id, schemaClean, qualifiedTable.get(),
                request.getRowid(), columnValues);
        if (err.isPresent()) {
            return new OracleRowDetailResponse(err.get(), List.of(), "");
        }
        Map<String, Object> result = oracleMetadataService.getDetailRowWithRowid(id, schemaClean, request.getSql(),
                request.getRowNum(), request.getSort() != null ? request.getSort() : "",
                request.getOrder() != null ? request.getOrder() : "");
        String error = (String) result.get("error");
        @SuppressWarnings("unchecked")
        List<Map<String, String>> detailRows = (List<Map<String, String>>) result.get("detailRows");
        String rowRowid = (String) result.get("rowRowid");
        return new OracleRowDetailResponse(error, detailRows != null ? detailRows : List.of(),
                rowRowid != null ? rowRowid : "");
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

    private static String unquote(String s) {
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
