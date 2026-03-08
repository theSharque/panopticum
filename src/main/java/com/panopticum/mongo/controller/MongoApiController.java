package com.panopticum.mongo.controller;

import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import io.micronaut.context.annotation.Value;
import com.panopticum.mongo.model.MongoCollectionInfo;
import com.panopticum.mongo.model.MongoDocumentReplaceRequest;
import com.panopticum.mongo.model.MongoQueryRequest;
import com.panopticum.mongo.service.MongoMetadataService;
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

import java.util.Map;
import java.util.Optional;

@Controller("/api/mongo/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Tag(name = "MongoDB", description = "MongoDB metadata and query API")
public class MongoApiController {

    private final DbConnectionService dbConnectionService;
    private final MongoMetadataService mongoMetadataService;

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
        return mongoMetadataService.listDatabasesPaged(id, page, size, sort, order);
    }

    @Get("/{id}/databases/{dbName}/collections")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List collections")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of collections"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<MongoCollectionInfo> collections(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return mongoMetadataService.listCollectionsPaged(id, dbName, page, size, sort, order);
    }

    @Post("/{id}/query")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute MQL query")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Query result"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public QueryResult query(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @Valid @Body MongoQueryRequest request) {
        ensureConnectionExists(id);
        String queryText = request.getQuery() != null && !request.getQuery().isBlank() ? request.getQuery() : "{}";
        int off = request.getOffset() != null ? Math.max(0, request.getOffset()) : 0;
        int lim = request.getLimit() != null && request.getLimit() > 0 ? Math.min(request.getLimit(), 1000) : 100;
        String sortVal = request.getSort() != null && !request.getSort().isBlank() ? request.getSort() : "_id";
        String orderVal = request.getOrder() != null && !request.getOrder().isBlank() ? request.getOrder() : "asc";
        return mongoMetadataService.executeQuery(id, request.getDbName(), request.getCollection(), queryText, off, lim, sortVal, orderVal)
                .orElse(QueryResult.error("error.queryExecutionFailed"));
    }

    @Get("/{id}/databases/{dbName}/document")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get document by ID")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Document JSON"),
            @ApiResponse(responseCode = "404", description = "connection.notFound or document not found")
    })
    public String document(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @QueryValue String collection,
            @QueryValue String docId) {
        ensureConnectionExists(id);
        return mongoMetadataService.getDocument(id, dbName, collection, docId)
                .map(mongoMetadataService::documentToPrettyJson)
                .orElse("{}");
    }

    @Post("/{id}/databases/{dbName}/document")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Replace document")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "400", description = "Error key in response"),
            @ApiResponse(responseCode = "403", description = "read.only.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Object replaceDocument(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String dbName,
            @Valid @Body MongoDocumentReplaceRequest request) {
        assertNotReadOnly();
        ensureConnectionExists(id);
        Optional<String> err = mongoMetadataService.replaceDocument(id, dbName, request.getCollection(),
                request.getDocId(), request.getBody());
        if (err.isPresent()) {
            return Map.of("error", err.get());
        }
        return Map.of("success", true);
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
