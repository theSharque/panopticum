package com.panopticum.mongo.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.mongo.service.MongoMetadataService;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.ModelAndView;
import io.micronaut.views.View;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/mongo")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
public class MongoController {

    private final DbConnectionService dbConnectionService;
    private final MongoMetadataService mongoMetadataService;

    public MongoController(DbConnectionService dbConnectionService, MongoMetadataService mongoMetadataService) {
        this.dbConnectionService = dbConnectionService;
        this.mongoMetadataService = mongoMetadataService;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("mongo/databases")
    public Map<String, Object> databases(@PathVariable Long id,
                                         @QueryValue(value = "page", defaultValue = "1") int page,
                                         @QueryValue(value = "size", defaultValue = "50") int size,
                                         @QueryValue(value = "sort", defaultValue = "name") String sort,
                                         @QueryValue(value = "order", defaultValue = "asc") String order) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", null);
        List<String> all = new ArrayList<>(mongoMetadataService.listDatabases(id));
        boolean desc = "desc".equalsIgnoreCase(order);
        all.sort(desc ? (a, b) -> b.compareToIgnoreCase(a) : (a, b) -> a.compareToIgnoreCase(b));
        int limit = Math.min(Math.max(1, size), 500);
        int offset = Math.max(0, (page - 1) * limit);
        List<String> dbs = offset < all.size() ? all.subList(offset, Math.min(offset + limit, all.size())) : List.of();
        model.put("items", dbs);
        model.put("itemType", "database");
        model.put("itemUrlPrefix", "/mongo/" + id + "/");
        model.put("page", page);
        model.put("size", limit);
        model.put("sort", sort != null ? sort : "name");
        model.put("order", order != null ? order : "asc");
        model.put("fromRow", all.isEmpty() ? 0 : offset + 1);
        model.put("toRow", offset + dbs.size());
        model.put("hasPrev", page > 1);
        model.put("hasMore", offset + dbs.size() < all.size());
        model.put("prevOffset", Math.max(0, offset - limit));
        model.put("nextOffset", offset + limit);

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}")
    @View("mongo/collections")
    public Map<String, Object> collections(@PathVariable Long id, @PathVariable String dbName,
                                          @QueryValue(value = "page", defaultValue = "1") int page,
                                          @QueryValue(value = "size", defaultValue = "50") int size,
                                          @QueryValue(value = "sort", defaultValue = "name") String sort,
                                          @QueryValue(value = "order", defaultValue = "asc") String order) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        int limit = Math.min(Math.max(1, size), 500);
        List<String> all = mongoMetadataService.listCollections(id, dbName, 0, 500);
        all = new ArrayList<>(all);
        boolean desc = "desc".equalsIgnoreCase(order);
        all.sort(desc ? (a, b) -> b.compareToIgnoreCase(a) : (a, b) -> a.compareToIgnoreCase(b));
        int offset = Math.max(0, (page - 1) * limit);
        List<String> collections = offset < all.size() ? all.subList(offset, Math.min(offset + limit, all.size())) : List.of();
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mongo/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("items", collections);
        model.put("itemType", "collection");
        model.put("itemUrlPrefix", "/mongo/" + id + "/" + dbName + "/query?collection=");
        model.put("page", page);
        model.put("size", limit);
        model.put("sort", sort != null ? sort : "name");
        model.put("order", order != null ? order : "asc");
        model.put("fromRow", all.isEmpty() ? 0 : offset + 1);
        model.put("toRow", offset + collections.size());
        model.put("hasPrev", page > 1);
        model.put("hasMore", offset + collections.size() < all.size());
        model.put("prevOffset", Math.max(0, offset - limit));
        model.put("nextOffset", offset + limit);

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/query")
    @View("mongo/query")
    public Map<String, Object> queryPageGet(@PathVariable Long id, @PathVariable String dbName,
                                            @QueryValue(value = "collection", defaultValue = "") String collection,
                                            @QueryValue(value = "query", defaultValue = "") String query,
                                            @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                            @QueryValue(value = "limit", defaultValue = "100") Integer limit) {
        return buildQueryPageModel(id, dbName, collection, query, offset, limit);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("mongo/query")
    public Map<String, Object> queryPagePost(@PathVariable Long id, @PathVariable String dbName,
                                             @Nullable String collection, @Nullable String query,
                                             @Nullable Integer offset, @Nullable Integer limit) {
        return buildQueryPageModel(id, dbName, collection, query, offset, limit);
    }

    private Map<String, Object> buildQueryPageModel(Long id, String dbName, String collection, String query,
                                                    Integer offset, Integer limit) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mongo/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/mongo/" + id + "/" + dbName));
        breadcrumbs.add(new BreadcrumbItem("query", null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("collection", collection != null ? collection : "");
        model.put("query", query != null ? query : "");
        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? Math.min(limit, 1000) : 100;
        model.put("size", lim);

        if (collection == null || collection.isBlank()) {
            model.put("error", null);
            model.put("columns", List.<String>of());
            model.put("rows", List.<List<Object>>of());
            model.put("docIds", List.<String>of());
            model.put("offset", 0);
            model.put("limit", lim);
            model.put("hasPrev", false);
            model.put("hasMore", false);
            model.put("nextOffset", lim);
            model.put("prevOffset", 0);
            model.put("fromRow", 0);
            model.put("toRow", 0);
        } else {
            String queryText = query != null && !query.isBlank() ? query : "{}";
            var result = mongoMetadataService.executeQuery(id, dbName, collection, queryText, off, lim)
                    .orElse(QueryResult.error("Ошибка выполнения запроса"));
            model.put("error", result.hasError() ? result.getError() : null);
            model.put("columns", result.getColumns());
            model.put("rows", result.getRows());
            model.put("docIds", result.getDocIds() != null ? result.getDocIds() : List.<String>of());
            model.put("offset", result.getOffset());
            model.put("limit", result.getLimit());
            model.put("hasPrev", result.hasPrev());
            model.put("hasMore", result.isHasMore());
            model.put("nextOffset", result.nextOffset());
            model.put("prevOffset", result.prevOffset());
            model.put("fromRow", result.fromRow());
            model.put("toRow", result.toRow());
        }

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/detail")
    @View("mongo/detail")
    public Map<String, Object> documentDetail(@PathVariable Long id, @PathVariable String dbName,
                                              @QueryValue(value = "collection", defaultValue = "") String collection,
                                              @QueryValue(value = "docId", defaultValue = "") String docId) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            model.put("prettyJson", "{}");
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mongo/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/mongo/" + id + "/" + dbName));
        breadcrumbs.add(new BreadcrumbItem(collection != null ? collection : "", "/mongo/" + id + "/" + dbName + "/query?collection=" + (collection != null ? collection : "")));
        breadcrumbs.add(new BreadcrumbItem("detail", null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("collection", collection != null ? collection : "");

        String json = mongoMetadataService.getDocument(id, dbName, collection, docId)
                .map(mongoMetadataService::documentToPrettyJson)
                .orElse("{}");
        model.put("prettyJson", json);

        return model;
    }

    @Post("/{id}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object executeQuery(@PathVariable Long id, String dbName, String collection, String query,
                               @Nullable Integer offset, @Nullable Integer limit, String target) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("collection", collection);
        if (collection == null || collection.isBlank()) {
            model.put("error", "Укажите коллекцию");
            model.put("docIds", List.<String>of());
            return "table".equals(target)
                    ? new ModelAndView<>("partials/mongo-table-view-result", model)
                    : new ModelAndView<>("partials/mongo-query-result", model);
        }

        String queryText = query != null && !query.isBlank() ? query : "{}";
        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = mongoMetadataService.executeQuery(id, dbName, collection, queryText, off, lim)
                .orElse(QueryResult.error("Execution failed"));

        model.put("error", result.hasError() ? result.getError() : null);
        model.put("columns", result.getColumns());
        model.put("rows", result.getRows());
        model.put("docIds", result.getDocIds() != null ? result.getDocIds() : List.<String>of());
        model.put("query", queryText);
        model.put("offset", result.getOffset());
        model.put("limit", result.getLimit());
        model.put("hasPrev", result.hasPrev());
        model.put("hasMore", result.isHasMore());
        model.put("nextOffset", result.nextOffset());
        model.put("prevOffset", result.prevOffset());
        model.put("fromRow", result.fromRow());
        model.put("toRow", result.toRow());

        return "table".equals(target)
                ? new ModelAndView<>("partials/mongo-table-view-result", model)
                : new ModelAndView<>("partials/mongo-query-result", model);
    }

    private Map<String, Object> baseModel(Long id) {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        dbConnectionService.findById(id).ifPresent(conn -> model.put("connection", conn));

        return model;
    }
}
