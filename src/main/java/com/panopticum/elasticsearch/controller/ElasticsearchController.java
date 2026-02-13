package com.panopticum.elasticsearch.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.elasticsearch.model.ElasticsearchIndexInfo;
import com.panopticum.elasticsearch.service.ElasticsearchService;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
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

@Controller("/elasticsearch")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class ElasticsearchController {

    private static final String DEFAULT_QUERY = "{\"query\":{\"match_all\":{}}}";
    private static final int DEFAULT_SIZE = 100;

    private final DbConnectionService dbConnectionService;
    private final ElasticsearchService elasticsearchService;

    @Value("${panopticum.read-only:false}")
    private boolean readOnly;

    @Get("/{id}")
    public HttpResponse<?> index(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/elasticsearch/" + id + "/indices"));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/indices")
    @View("elasticsearch/indices")
    public Map<String, Object> indices(@PathVariable Long id,
                                       @QueryValue(value = "page", defaultValue = "1") int page,
                                       @QueryValue(value = "size", defaultValue = "50") int size,
                                       @QueryValue(value = "sort", defaultValue = "index") String sort,
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

        Page<ElasticsearchIndexInfo> paged = elasticsearchService.listIndicesPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("index", "orderIndex", "docsCount", "orderDocsCount", "storeSize", "orderStoreSize"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/indices/{indexName}/search")
    @View("elasticsearch/search")
    public Map<String, Object> searchPage(@PathVariable Long id, @PathVariable String indexName,
                                         @QueryValue(value = "query", defaultValue = "") String query,
                                         @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                         @QueryValue(value = "limit", defaultValue = "100") Integer limit) {
        return buildSearchPageModel(id, indexName, query, offset, limit);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/indices/{indexName}/search")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("elasticsearch/search")
    public Map<String, Object> searchPagePost(@PathVariable Long id, @PathVariable String indexName,
                                             @Nullable String query, @Nullable Integer offset, @Nullable Integer limit) {
        return buildSearchPageModel(id, indexName, query, offset, limit);
    }

    private Map<String, Object> buildSearchPageModel(Long id, String indexName, String query,
                                                    Integer offset, Integer limit) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/elasticsearch/" + id + "/indices"));
        breadcrumbs.add(new BreadcrumbItem(indexName != null ? indexName : "", null));
        breadcrumbs.add(new BreadcrumbItem("search", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("indexName", indexName != null ? indexName : "");
        model.put("indexNameEncoded", indexName != null ? encodePath(indexName) : "");
        model.put("query", query != null && !query.isBlank() ? query : DEFAULT_QUERY);
        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? Math.min(limit, 1000) : DEFAULT_SIZE;
        model.put("size", lim);
        model.put("offset", off);

        if (indexName == null || indexName.isBlank()) {
            model.put("error", null);
            model.put("columns", List.<String>of());
            model.put("rows", List.<List<Object>>of());
            model.put("docIds", List.<String>of());
            model.put("hasPrev", false);
            model.put("hasMore", false);
            model.put("nextOffset", lim);
            model.put("prevOffset", 0);
            model.put("fromRow", 0);
            model.put("toRow", 0);
        } else {
            var result = elasticsearchService.search(id, indexName, query, off, lim)
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
    @Get("/{id}/indices/{indexName}/doc/{docId}")
    @View("elasticsearch/detail")
    public Map<String, Object> documentDetail(@PathVariable Long id, @PathVariable String indexName,
                                             @PathVariable String docId) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            model.put("prettyJson", "{}");
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/elasticsearch/" + id + "/indices"));
        breadcrumbs.add(new BreadcrumbItem(indexName != null ? indexName : "",
                "/elasticsearch/" + id + "/indices/" + (indexName != null ? encodePath(indexName) : "") + "/search"));
        breadcrumbs.add(new BreadcrumbItem("detail", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("indexName", indexName != null ? indexName : "");
        model.put("indexNameEncoded", indexName != null ? encodePath(indexName) : "");
        model.put("docId", docId != null ? docId : "");
        model.put("docIdEncoded", docId != null ? encodePath(docId) : "");
        model.put("readOnly", readOnly);

        String json = elasticsearchService.getDocument(id, indexName, docId).orElse("{}");
        model.put("prettyJson", json);
        model.put("error", null);

        return model;
    }

    @Post("/{id}/indices/{indexName}/doc/{docId}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveDocument(@PathVariable Long id, @PathVariable String indexName, @PathVariable String docId,
                              String body) {
        assertNotReadOnly();
        Optional<String> err = elasticsearchService.updateDocument(id, indexName, docId, body);

        if (err.isPresent()) {
            Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
            Optional<DbConnection> conn = dbConnectionService.findById(id);
            if (conn.isEmpty()) {
                model.put("indexName", indexName != null ? indexName : "");
                model.put("indexNameEncoded", encodePath(indexName != null ? indexName : ""));
                model.put("docId", docId != null ? docId : "");
                model.put("docIdEncoded", encodePath(docId != null ? docId : ""));
                model.put("prettyJson", body != null ? body : "{}");
                model.put("error", err.get());
                model.put("readOnly", readOnly);
                model.put("breadcrumbs", List.<BreadcrumbItem>of());
                return new ModelAndView<>("elasticsearch/detail", model);
            }

            List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
            breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/elasticsearch/" + id + "/indices"));
            breadcrumbs.add(new BreadcrumbItem(indexName != null ? indexName : "",
                    "/elasticsearch/" + id + "/indices/" + (indexName != null ? encodePath(indexName) : "") + "/search"));
            breadcrumbs.add(new BreadcrumbItem("detail", null));
            ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
            model.put("connectionId", id);
            model.put("indexName", indexName != null ? indexName : "");
            model.put("indexNameEncoded", indexName != null ? encodePath(indexName) : "");
            model.put("docId", docId != null ? docId : "");
            model.put("docIdEncoded", docId != null ? encodePath(docId) : "");
            model.put("readOnly", readOnly);
            model.put("prettyJson", body != null ? body : "{}");
            model.put("error", err.get());
            return new ModelAndView<>("elasticsearch/detail", model);
        }

        return HttpResponse.redirect(URI.create("/elasticsearch/" + id + "/indices/" + encodePath(indexName)
                + "/doc/" + encodePath(docId)));
    }

    @Post("/{id}/search")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object executeSearch(@PathVariable Long id, String indexName, String query,
                               @Nullable Integer offset, @Nullable Integer limit, String target) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("indexName", indexName != null ? indexName : "");
        model.put("indexNameEncoded", indexName != null ? encodePath(indexName) : "");
        model.put("query", query != null && !query.isBlank() ? query : DEFAULT_QUERY);
        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? Math.min(limit, 1000) : DEFAULT_SIZE;

        if (indexName == null || indexName.isBlank()) {
            model.put("error", "error.specifyIndex");
            model.put("docIds", List.<String>of());
            model.put("columns", List.<String>of());
            model.put("rows", List.<List<Object>>of());
            model.put("offset", 0);
            model.put("limit", lim);
            model.put("hasPrev", false);
            model.put("hasMore", false);
            model.put("nextOffset", lim);
            model.put("prevOffset", 0);
            model.put("fromRow", 0);
            model.put("toRow", 0);
            return new ModelAndView<>("partials/elasticsearch-search-result", model);
        }

        var result = elasticsearchService.search(id, indexName, query, off, lim)
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

        return new ModelAndView<>("partials/elasticsearch-search-result", model);
    }

    private void assertNotReadOnly() {
        if (readOnly) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "read.only.enabled");
        }
    }

    private static String encodePath(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
