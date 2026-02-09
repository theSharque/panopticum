package com.panopticum.cassandra.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.cassandra.model.CassandraKeyspaceInfo;
import com.panopticum.cassandra.model.CassandraTableInfo;
import com.panopticum.cassandra.service.CassandraMetadataService;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/cassandra")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class CassandraController {

    private final DbConnectionService dbConnectionService;
    private final CassandraMetadataService cassandraMetadataService;

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("cassandra/keyspaces")
    public Map<String, Object> keyspaces(@PathVariable Long id,
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
        model.put("keyspaceName", null);
        model.put("itemType", "keyspace");
        model.put("itemUrlPrefix", "/cassandra/" + id + "/");

        Page<CassandraKeyspaceInfo> paged = cassandraMetadataService.listKeyspacesPaged(id, page, size, sort, order);
        String orderVal = paged.getOrder();
        String sortBy = paged.getSort();
        model.put("items", paged.getItems());
        model.put("page", paged.getPage());
        model.put("size", paged.getSize());
        model.put("sort", sortBy);
        model.put("order", orderVal);
        model.put("orderName", "name".equals(sortBy) && "asc".equals(orderVal) ? "desc" : "asc");
        model.put("orderDurableWrites", "durableWrites".equals(sortBy) && "asc".equals(orderVal) ? "desc" : "asc");
        model.put("orderReplication", "replication".equals(sortBy) && "asc".equals(orderVal) ? "desc" : "asc");
        model.put("fromRow", paged.getFromRow());
        model.put("toRow", paged.getToRow());
        model.put("hasPrev", paged.isHasPrev());
        model.put("hasMore", paged.isHasMore());
        model.put("prevOffset", paged.getPrevOffset());
        model.put("nextOffset", paged.getNextOffset());

        return model;
    }

    @Get("/{id}/detail")
    public HttpResponse<?> detailRedirect(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/cassandra/" + id));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{keyspaceName}")
    @View("cassandra/tables")
    public Map<String, Object> tables(@PathVariable Long id, @PathVariable String keyspaceName,
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
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/cassandra/" + id));
        breadcrumbs.add(new BreadcrumbItem(keyspaceName, null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("keyspaceName", keyspaceName);

        Page<CassandraTableInfo> paged = cassandraMetadataService.listTablesPaged(id, keyspaceName, page, size, sort, order);
        String orderVal = paged.getOrder();
        String sortBy = paged.getSort();
        model.put("tables", paged.getItems());
        model.put("page", paged.getPage());
        model.put("size", paged.getSize());
        model.put("sort", sortBy);
        model.put("order", orderVal);
        model.put("orderName", "name".equals(sortBy) && "asc".equals(orderVal) ? "desc" : "asc");
        model.put("orderType", "type".equals(sortBy) && "asc".equals(orderVal) ? "desc" : "asc");
        model.put("orderComment", "comment".equals(sortBy) && "asc".equals(orderVal) ? "desc" : "asc");
        model.put("orderTtl", "ttl".equals(sortBy) && "asc".equals(orderVal) ? "desc" : "asc");
        model.put("orderGcGrace", "gcGrace".equals(sortBy) && "asc".equals(orderVal) ? "desc" : "asc");
        model.put("fromRow", paged.getFromRow());
        model.put("toRow", paged.getToRow());
        model.put("hasPrev", paged.isHasPrev());
        model.put("hasMore", paged.isHasMore());
        model.put("prevOffset", paged.getPrevOffset());
        model.put("nextOffset", paged.getNextOffset());

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{keyspaceName}/cql")
    @View("cassandra/cql")
    public Map<String, Object> cqlPageGet(@PathVariable Long id, @PathVariable String keyspaceName,
                                         @QueryValue(value = "sql", defaultValue = "") String sql,
                                         @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                         @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                         @QueryValue(value = "sort", defaultValue = "") String sort,
                                         @QueryValue(value = "order", defaultValue = "") String order) {
        return buildCqlPageModel(id, keyspaceName, sql, offset, limit, sort, order);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{keyspaceName}/cql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("cassandra/cql")
    public Map<String, Object> cqlPagePost(@PathVariable Long id, @PathVariable String keyspaceName,
                                           String sql, @Nullable Integer offset, @Nullable Integer limit,
                                           @Nullable String sort, @Nullable String order) {
        return buildCqlPageModel(id, keyspaceName, sql, offset, limit, sort, order);
    }

    private Map<String, Object> buildCqlPageModel(Long id, String keyspaceName, String sql,
                                                  Integer offset, Integer limit, String sort, String order) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/cassandra/" + id));
        breadcrumbs.add(new BreadcrumbItem(keyspaceName, "/cassandra/" + id + "/" + keyspaceName));
        breadcrumbs.add(new BreadcrumbItem("CQL", null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("keyspaceName", keyspaceName);
        model.put("sql", sql != null ? sql : "");

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? Math.min(limit, 1000) : 100;
        model.put("size", lim);

        if (sql == null || sql.isBlank()) {
            model.put("error", null);
            model.put("columns", List.<String>of());
            model.put("columnTypes", List.<String>of());
            model.put("rows", List.<List<Object>>of());
            model.put("offset", 0);
            model.put("limit", lim);
            model.put("hasPrev", false);
            model.put("hasMore", false);
            model.put("nextOffset", lim);
            model.put("prevOffset", 0);
            model.put("fromRow", 0);
            model.put("toRow", 0);
            model.put("sort", "");
            model.put("order", "");
        } else {
            var result = cassandraMetadataService.executeQuery(id, keyspaceName, sql, off, lim)
                    .orElse(QueryResult.error("error.queryExecutionFailed"));
            model.put("error", result.hasError() ? result.getError() : null);
            model.put("columns", result.getColumns());
            model.put("columnTypes", result.getColumnTypes() != null ? result.getColumnTypes() : List.<String>of());
            model.put("rows", result.getRows());
            model.put("offset", result.getOffset());
            model.put("limit", result.getLimit());
            model.put("hasPrev", result.hasPrev());
            model.put("hasMore", result.isHasMore());
            model.put("nextOffset", result.nextOffset());
            model.put("prevOffset", result.prevOffset());
            model.put("fromRow", result.fromRow());
            model.put("toRow", result.toRow());
            model.put("sort", sort != null ? sort : "");
            model.put("order", order != null ? order : "");
        }

        return model;
    }

    @Post("/{id}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object executeQuery(@PathVariable Long id, String sql, String keyspaceName,
                               @Nullable Integer offset, @Nullable Integer limit,
                               @Nullable String sort, @Nullable String order, String target) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("keyspaceName", keyspaceName);
        if (sql == null || sql.isBlank()) {
            model.put("error", "Empty query");

            return "table".equals(target)
                    ? new ModelAndView<>("partials/cassandra-table-view-result", model)
                    : new ModelAndView<>("partials/cassandra-query-result", model);
        }

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = cassandraMetadataService.executeQuery(id, keyspaceName, sql, off, lim)
                .orElse(QueryResult.error("Execution failed"));
        model.put("error", result.hasError() ? result.getError() : null);
        model.put("columns", result.getColumns());
        model.put("columnTypes", result.getColumnTypes() != null ? result.getColumnTypes() : List.<String>of());
        model.put("rows", result.getRows());
        model.put("sql", sql);
        model.put("offset", result.getOffset());
        model.put("limit", result.getLimit());
        model.put("hasPrev", result.hasPrev());
        model.put("hasMore", result.isHasMore());
        model.put("nextOffset", result.nextOffset());
        model.put("prevOffset", result.prevOffset());
        model.put("fromRow", result.fromRow());
        model.put("toRow", result.toRow());
        model.put("sort", sort != null ? sort : "");
        model.put("order", order != null ? order : "");

        return "table".equals(target)
                ? new ModelAndView<>("partials/cassandra-table-view-result", model)
                : new ModelAndView<>("partials/cassandra-query-result", model);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{keyspaceName}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("cassandra/detail")
    public Map<String, Object> rowDetail(@PathVariable Long id, @PathVariable String keyspaceName,
                                        String sql, Integer rowNum, String sort, String order) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/cassandra/" + id));
        breadcrumbs.add(new BreadcrumbItem(keyspaceName != null ? keyspaceName : "", "/cassandra/" + id + "/" + (keyspaceName != null ? keyspaceName : "")));
        breadcrumbs.add(new BreadcrumbItem("detail", null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("keyspaceName", keyspaceName != null ? keyspaceName : "");
        model.put("sql", sql != null ? sql : "");
        model.put("rowNum", rowNum != null ? rowNum : 0);
        model.put("sort", sort != null ? sort : "");
        model.put("order", order != null ? order : "");

        if (sql != null && !sql.isBlank() && rowNum != null && rowNum >= 0) {
            Map<String, Object> detailResult = cassandraMetadataService.getDetailRow(id, keyspaceName, sql,
                    Math.max(0, rowNum), sort, order);
            model.putAll(detailResult);
        } else {
            model.put("detailRows", List.<Map<String, String>>of());
            model.put("editable", false);
        }

        return model;
    }

    @Post("/{id}/{keyspaceName}/detail/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveRow(@PathVariable Long id, @PathVariable String keyspaceName,
                         @Body Map<String, String> form) {
        String sql = form != null ? form.get("sql") : null;
        Integer rowNum = form != null && form.containsKey("rowNum") ? parseInteger(form.get("rowNum")) : null;
        String sort = form != null ? form.get("sort") : null;
        String order = form != null ? form.get("order") : null;

        List<String> uniqueKeyColumns = new ArrayList<>();
        if (form != null && form.containsKey("uniqueKeyColumns")) {
            String raw = form.get("uniqueKeyColumns");
            if (raw != null && !raw.isBlank()) {
                for (String s : raw.split(",")) {
                    String t = s.trim();
                    if (!t.isBlank()) {
                        uniqueKeyColumns.add(t);
                    }
                }
            }
        }

        Map<String, String> columnValues = new LinkedHashMap<>();
        Map<String, Object> keyValues = new LinkedHashMap<>();
        if (form != null) {
            for (Map.Entry<String, String> e : form.entrySet()) {
                if (e.getKey() != null && e.getKey().startsWith("field_")) {
                    String colName = e.getKey().substring(6);
                    String val = e.getValue() != null ? e.getValue() : "";
                    if (uniqueKeyColumns.contains(colName)) {
                        keyValues.put(colName, val.isEmpty() ? null : val);
                    } else {
                        columnValues.put(colName, val);
                    }
                }
            }
        }

        Optional<String> tableOpt = cassandraMetadataService.parseTableFromCql(sql);
        if (tableOpt.isEmpty()) {
            Map<String, Object> model = rowDetail(id, keyspaceName, sql, rowNum, sort, order);
            model.put("error", "Could not determine table from SQL.");
            return new ModelAndView<>("cassandra/detail", model);
        }

        Optional<String> err = cassandraMetadataService.executeUpdateByKey(id, keyspaceName, tableOpt.get(),
                uniqueKeyColumns, keyValues, columnValues);
        if (err.isPresent()) {
            Map<String, Object> model = rowDetail(id, keyspaceName, sql, rowNum, sort, order);
            model.put("error", err.get());
            return new ModelAndView<>("cassandra/detail", model);
        }

        return new ModelAndView<>("cassandra/detail", rowDetail(id, keyspaceName, sql, rowNum, sort, order));
    }

    private static Integer parseInteger(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            return Integer.valueOf(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Map<String, Object> baseModel(Long id) {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        dbConnectionService.findById(id).ifPresent(conn -> model.put("connection", conn));

        return model;
    }
}
