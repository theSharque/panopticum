package com.panopticum.mongo.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.mongo.model.MongoCollectionInfo;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.mongo.service.MongoMetadataService;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
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
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/mongo")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class MongoController {

    private final DbConnectionService dbConnectionService;
    private final MongoMetadataService mongoMetadataService;

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("mongo/databases")
    public Map<String, Object> databases(@PathVariable Long id,
                                         @QueryValue(value = "page", defaultValue = "1") int page,
                                         @QueryValue(value = "size", defaultValue = "50") int size,
                                         @QueryValue(value = "sort", defaultValue = "name") String sort,
                                         @QueryValue(value = "order", defaultValue = "asc") String order) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", null);
        model.put("itemType", "database");
        model.put("itemUrlPrefix", "/mongo/" + id + "/");

        Page<DatabaseInfo> paged = mongoMetadataService.listDatabasesPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "size", "orderSize"));

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
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mongo/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("itemType", "collection");
        model.put("itemUrlPrefix", "/mongo/" + id + "/" + dbName + "/query?collection=");

        Page<MongoCollectionInfo> paged = mongoMetadataService.listCollectionsPaged(id, dbName, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "count", "orderCount", "size", "orderSize"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/query")
    public HttpResponse<?> queryRedirect(@PathVariable Long id, @PathVariable String dbName) {
        return HttpResponse.redirect(URI.create("/mongo/" + id + "/" + dbName));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/{collection}/query")
    @View("mongo/query")
    public Map<String, Object> queryPageGet(@PathVariable Long id, @PathVariable String dbName, @PathVariable String collection,
                                            @QueryValue(value = "query", defaultValue = "") String query,
                                            @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                            @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                            @QueryValue(value = "sort", defaultValue = "_id") String sort,
                                            @QueryValue(value = "order", defaultValue = "asc") String order) {
        return buildQueryPageModel(id, dbName, collection, query, offset, limit, sort, order);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/{collection}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("mongo/query")
    public Map<String, Object> queryPagePost(@PathVariable Long id, @PathVariable String dbName, @PathVariable String collection,
                                             @Nullable String query, @Nullable Integer offset, @Nullable Integer limit,
                                             @Nullable String sort, @Nullable String order) {
        return buildQueryPageModel(id, dbName, collection, query, offset, limit, sort, order);
    }

    private Map<String, Object> buildQueryPageModel(Long id, String dbName, String collection, String query,
                                                    Integer offset, Integer limit, String sort, String order) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mongo/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/mongo/" + id + "/" + dbName));
        String collectionUrl = (collection != null && !collection.isBlank())
                ? "/mongo/" + id + "/" + dbName + "/" + collection + "/query"
                : null;
        breadcrumbs.add(new BreadcrumbItem(collection != null ? collection : "", collectionUrl));
        breadcrumbs.add(new BreadcrumbItem("query", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("collection", collection != null ? collection : "");
        model.put("query", query != null ? query : "");
        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? Math.min(limit, 1000) : 100;
        model.put("size", lim);
        String sortVal = sort != null && !sort.isBlank() ? sort : "_id";
        String orderVal = order != null && !order.isBlank() ? order : "asc";
        model.put("sort", sortVal);
        model.put("order", orderVal);

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
            var result = mongoMetadataService.executeQuery(id, dbName, collection, queryText, off, lim, sortVal, orderVal)
                    .orElse(QueryResult.error("error.queryExecutionFailed"));
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
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            model.put("prettyJson", "{}");

            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mongo/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/mongo/" + id + "/" + dbName));
        String collectionDetailUrl = (collection != null && !collection.isBlank())
                ? "/mongo/" + id + "/" + dbName + "/" + collection + "/query"
                : null;
        breadcrumbs.add(new BreadcrumbItem(collection != null ? collection : "", collectionDetailUrl));
        breadcrumbs.add(new BreadcrumbItem("detail", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("collection", collection != null ? collection : "");
        model.put("docId", docId != null ? docId : "");

        String json = mongoMetadataService.getDocument(id, dbName, collection, docId)
                .map(mongoMetadataService::documentToPrettyJson)
                .orElse("{}");
        model.put("prettyJson", json);

        return model;
    }

    @Post("/{id}/{dbName}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveDocument(@PathVariable Long id, @PathVariable String dbName, String collection, String docId, String body) {
        Optional<String> err = mongoMetadataService.replaceDocument(id, dbName, collection, docId, body);

        if (err.isPresent()) {
            Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
            Optional<DbConnection> conn = dbConnectionService.findById(id);

            if (conn.isEmpty()) {
                model.put("prettyJson", body != null ? body : "{}");
                model.put("error", err.get());
                return new ModelAndView<>("mongo/detail", model);
            }

            List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
            breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mongo/" + id));
            breadcrumbs.add(new BreadcrumbItem(dbName, "/mongo/" + id + "/" + dbName));
            String collectionDetailUrl = (collection != null && !collection.isBlank())
                    ? "/mongo/" + id + "/" + dbName + "/" + collection + "/query"
                    : null;
            breadcrumbs.add(new BreadcrumbItem(collection != null ? collection : "", collectionDetailUrl));
            breadcrumbs.add(new BreadcrumbItem("detail", null));
            ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
            model.put("connectionId", id);
            model.put("dbName", dbName);
            model.put("collection", collection != null ? collection : "");
            model.put("docId", docId != null ? docId : "");
            model.put("prettyJson", body != null ? body : "{}");
            model.put("error", err.get());
            return new ModelAndView<>("mongo/detail", model);
        }

        String q = (collection != null && !collection.isBlank() && docId != null && !docId.isBlank())
                ? "?collection=" + URLEncoder.encode(collection, StandardCharsets.UTF_8)
                + "&docId=" + URLEncoder.encode(docId, StandardCharsets.UTF_8)
                : "";
        return HttpResponse.redirect(URI.create("/mongo/" + id + "/" + dbName + "/detail" + q));
    }

    @Post("/{id}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object executeQuery(@PathVariable Long id, String dbName, String collection, String query,
                               @Nullable Integer offset, @Nullable Integer limit, @Nullable String sort, @Nullable String order, String target) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("collection", collection);
        String sortVal = sort != null && !sort.isBlank() ? sort : "_id";
        String orderVal = order != null && !order.isBlank() ? order : "asc";
        model.put("sort", sortVal);
        model.put("order", orderVal);
        if (collection == null || collection.isBlank()) {
            model.put("error", "error.specifyCollection");
            model.put("docIds", List.<String>of());

            return "table".equals(target)
                    ? new ModelAndView<>("partials/mongo-table-view-result", model)
                    : new ModelAndView<>("partials/mongo-query-result", model);
        }

        String queryText = query != null && !query.isBlank() ? query : "{}";
        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = mongoMetadataService.executeQuery(id, dbName, collection, queryText, off, lim, sortVal, orderVal)
                .orElse(QueryResult.error("error.queryExecutionFailed"));

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
}
