package com.panopticum.elasticsearch.controller;

import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.elasticsearch.model.ElasticsearchIndexInfo;
import com.panopticum.elasticsearch.model.ElasticsearchSearchRequest;
import com.panopticum.elasticsearch.service.ElasticsearchService;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Put;
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

import java.util.Map;
import java.util.Optional;

@Controller("/api/elasticsearch/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Tag(name = "Elasticsearch", description = "Elasticsearch indices, search and documents API")
public class ElasticsearchApiController {

    private static final String DEFAULT_QUERY = "{\"query\":{\"match_all\":{}}}";

    private final DbConnectionService dbConnectionService;
    private final ElasticsearchService elasticsearchService;

    @Value("${panopticum.read-only:false}")
    private boolean readOnly;

    @Get("/{id}/indices")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List indices")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Indices page"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<ElasticsearchIndexInfo> indices(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "index") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return elasticsearchService.listIndicesPaged(id, page, size, sort, order);
    }

    @Post("/{id}/indices/{indexName}/search")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Search index")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Search result"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public QueryResult search(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String indexName,
            @Valid @Body ElasticsearchSearchRequest request) {
        ensureConnectionExists(id);
        String query = request.getQuery() != null && !request.getQuery().isBlank()
                ? request.getQuery() : DEFAULT_QUERY;
        int offset = Math.max(0, request.getOffset());
        int limit = request.getLimit() > 0 ? Math.min(request.getLimit(), 1000) : 100;
        return elasticsearchService.executeQuery(id, indexName, query, offset, limit)
                .orElse(QueryResult.error("error.queryExecutionFailed"));
    }

    @Get("/{id}/indices/{indexName}/doc/{docId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get document")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Document JSON"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public String documentDetail(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String indexName,
            @PathVariable String docId) {
        ensureConnectionExists(id);
        return elasticsearchService.getDocument(id, indexName, docId).orElse("{}");
    }

    @Put("/{id}/indices/{indexName}/doc/{docId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Update document")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Success or error key"),
            @ApiResponse(responseCode = "403", description = "read.only.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Map<String, Object> updateDocument(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String indexName,
            @PathVariable String docId,
            @Body String body) {
        assertNotReadOnly();
        ensureConnectionExists(id);
        Optional<String> err = elasticsearchService.updateDocument(id, indexName, docId, body);
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
