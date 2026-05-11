package com.panopticum.couchbase.controller;

import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.SqlQueryRequest;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.controller.AbstractConnectionApiController;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.ApiQueryParams;
import com.panopticum.couchbase.model.CouchbaseBucketInfo;
import com.panopticum.couchbase.model.CouchbaseScopeCollections;
import com.panopticum.couchbase.service.CouchbaseService;
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

import java.util.List;
import java.util.Map;

@Controller("/api/couchbase/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@Tag(name = "Couchbase", description = "Couchbase buckets, collections, and N1QL API")
public class CouchbaseApiController extends AbstractConnectionApiController {

    private final CouchbaseService couchbaseService;

    public CouchbaseApiController(DbConnectionService dbConnectionService, CouchbaseService couchbaseService) {
        super(dbConnectionService);
        this.couchbaseService = couchbaseService;
    }

    @Get("/{id}/buckets")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List buckets")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Buckets page"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<CouchbaseBucketInfo> buckets(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return couchbaseService.listBucketsPaged(id, page, size, sort, order);
    }

    @Get("/{id}/buckets/{bucket}/scopes")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List scopes and collections")
    public List<CouchbaseScopeCollections> scopes(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String bucket) {
        ensureConnectionExists(id);
        return couchbaseService.listScopesAndCollections(id, bucket);
    }

    @Get("/{id}/buckets/{bucket}/schemas")
    @Produces(MediaType.APPLICATION_JSON)
    public Page<SchemaInfo> schemas(
            @PathVariable Long id,
            @PathVariable String bucket,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return couchbaseService.listScopesPaged(id, bucket, page, size, sort, order);
    }

    @Get("/{id}/buckets/{bucket}/scopes/{scope}/tables")
    @Produces(MediaType.APPLICATION_JSON)
    public Page<TableInfo> collections(
            @PathVariable Long id,
            @PathVariable String bucket,
            @PathVariable String scope,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return couchbaseService.listCollectionsAsTablesPaged(id, bucket, scope, page, size, sort, order);
    }

    @Post("/{id}/query")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute N1QL")
    public QueryResult query(@PathVariable Long id, @Valid @Body SqlQueryRequest request) {
        ensureConnectionExists(id);
        if (request.getSql() == null || request.getSql().isBlank()) {
            return QueryResult.error("Empty query");
        }
        int offset = ApiQueryParams.normalizedOffset(request.getOffset());
        int limit = ApiQueryParams.normalizedLimit(request.getLimit());
        return couchbaseService.executeN1ql(id, request.getSql(), offset, limit);
    }

    @Get("/{id}/buckets/{bucket}/scopes/{scope}/collections/{collection}/documents")
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResult scan(
            @PathVariable Long id,
            @PathVariable String bucket,
            @PathVariable String scope,
            @PathVariable String collection,
            @QueryValue(value = "offset", defaultValue = "0") int offset,
            @QueryValue(value = "limit", defaultValue = "50") int limit) {
        ensureConnectionExists(id);
        return couchbaseService.scanCollection(id, bucket, scope, collection, offset, limit);
    }

    @Get("/{id}/buckets/{bucket}/scopes/{scope}/collections/{collection}/documents/{documentId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> doc(
            @PathVariable Long id,
            @PathVariable String bucket,
            @PathVariable String scope,
            @PathVariable String collection,
            @PathVariable String documentId) {
        ensureConnectionExists(id);
        return couchbaseService.getDocument(id, bucket, scope, collection, documentId)
                .orElse(Map.of());
    }
}
