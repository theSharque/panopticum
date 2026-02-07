package com.panopticum.redis.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.redis.model.RedisDbInfo;
import com.panopticum.redis.model.RedisKeyDetail;
import com.panopticum.redis.model.RedisKeysPage;
import com.panopticum.redis.service.RedisMetadataService;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.View;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/redis")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
public class RedisController {

    private final DbConnectionService dbConnectionService;
    private final RedisMetadataService redisMetadataService;

    public RedisController(DbConnectionService dbConnectionService, RedisMetadataService redisMetadataService) {
        this.dbConnectionService = dbConnectionService;
        this.redisMetadataService = redisMetadataService;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("redis/databases")
    public Map<String, Object> databases(@PathVariable Long id) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        List<RedisDbInfo> items = redisMetadataService.listDatabases(id);
        model.put("items", items);
        model.put("itemUrlPrefix", "/redis/" + id + "/");

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbIndex}")
    @View("redis/keys")
    public Map<String, Object> keys(@PathVariable Long id, @PathVariable int dbIndex,
                                    @QueryValue(value = "cursor", defaultValue = "0") String cursor,
                                    @QueryValue(value = "pattern", defaultValue = "*") String pattern,
                                    @QueryValue(value = "size", defaultValue = "100") int size) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/redis/" + id));
        breadcrumbs.add(new BreadcrumbItem("DB " + dbIndex, null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbIndex", dbIndex);
        model.put("pattern", pattern != null ? pattern : "*");

        RedisKeysPage page = redisMetadataService.listKeys(id, dbIndex, pattern, cursor, size);
        model.put("items", page.getKeys());
        model.put("nextCursor", page.getNextCursor());
        model.put("hasMore", page.isHasMore());
        model.put("cursor", cursor != null ? cursor : "0");

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbIndex}/detail")
    @View("redis/detail")
    public Map<String, Object> keyDetail(@PathVariable Long id, @PathVariable int dbIndex,
                                         @QueryValue("key") String key) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/redis/" + id));
        breadcrumbs.add(new BreadcrumbItem("DB " + dbIndex, "/redis/" + id + "/" + dbIndex));
        breadcrumbs.add(new BreadcrumbItem(key != null ? key : "", null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbIndex", dbIndex);
        model.put("key", key != null ? key : "");

        Optional<RedisKeyDetail> detail = redisMetadataService.getKeyDetail(id, dbIndex, key);
        model.put("keyDetail", detail.orElse(null));

        return model;
    }

    private Map<String, Object> baseModel(Long id) {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        dbConnectionService.findById(id).ifPresent(conn -> model.put("connection", conn));

        return model;
    }
}
