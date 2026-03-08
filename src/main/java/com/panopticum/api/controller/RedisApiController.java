package com.panopticum.api.controller;

import com.panopticum.api.model.RedisKeySaveRequest;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.redis.model.RedisDbInfo;
import com.panopticum.redis.model.RedisKeyDetail;
import com.panopticum.redis.model.RedisKeyInfo;
import com.panopticum.redis.model.RedisKeysPage;
import com.panopticum.redis.service.RedisMetadataService;
import io.micronaut.context.annotation.Value;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/api/redis/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Tag(name = "Redis", description = "Redis metadata and key API")
public class RedisApiController {

    private final DbConnectionService dbConnectionService;
    private final RedisMetadataService redisMetadataService;

    @Value("${panopticum.read-only:false}")
    private boolean readOnly;

    @Get("/{id}/databases")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List databases")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "List of database infos"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public List<RedisDbInfo> databases(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @QueryValue(value = "sort", defaultValue = "dbIndex") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return redisMetadataService.listDatabasesSorted(id, sort, order);
    }

    @Get("/{id}/databases/{dbIndex}/keys")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List keys (SCAN)")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Keys page"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Map<String, Object> keys(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable int dbIndex,
            @QueryValue(value = "cursor", defaultValue = "0") String cursor,
            @QueryValue(value = "search", defaultValue = "") String search,
            @QueryValue(value = "size", defaultValue = "100") int size,
            @QueryValue(value = "sort", defaultValue = "key") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        String pattern = search != null && !search.isBlank() ? "*" + search.trim() + "*" : "*";
        RedisKeysPage page = redisMetadataService.listKeys(id, dbIndex, pattern, cursor, size);
        List<RedisKeyInfo> items = redisMetadataService.sortKeys(page.getKeys(), sort, order);
        Map<String, Object> result = new HashMap<>();
        result.put("items", items);
        result.put("nextCursor", page.getNextCursor());
        result.put("hasMore", page.isHasMore());
        return result;
    }

    @Get("/{id}/databases/{dbIndex}/keys/detail")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get key detail")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Key detail or null"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public RedisKeyDetail keyDetail(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable int dbIndex,
            @QueryValue String key) {
        ensureConnectionExists(id);
        return redisMetadataService.getKeyDetail(id, dbIndex, key).orElse(null);
    }

    @Post("/{id}/databases/{dbIndex}/keys")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Save key (set string or hash)")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "400", description = "Error key in response"),
            @ApiResponse(responseCode = "403", description = "read.only.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Map<String, Object> saveKey(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable int dbIndex,
            @Valid @Body RedisKeySaveRequest request) {
        assertNotReadOnly();
        ensureConnectionExists(id);
        Optional<String> err;
        if ("hash".equalsIgnoreCase(request.getType() != null ? request.getType() : "")) {
            Map<String, String> fields = request.getFields() != null ? request.getFields() : Map.of();
            err = redisMetadataService.setHash(id, dbIndex, request.getKey(), fields);
        } else {
            err = redisMetadataService.setKey(id, dbIndex, request.getKey(),
                    request.getValue() != null ? request.getValue() : "");
        }
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
