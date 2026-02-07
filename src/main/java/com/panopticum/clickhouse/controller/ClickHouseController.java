package com.panopticum.clickhouse.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.clickhouse.model.ChDatabaseInfo;
import com.panopticum.clickhouse.model.ChTableInfo;
import com.panopticum.clickhouse.service.ClickHouseMetadataService;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/ch")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
public class ClickHouseController {

    private final DbConnectionService dbConnectionService;
    private final ClickHouseMetadataService clickHouseMetadataService;

    public ClickHouseController(DbConnectionService dbConnectionService,
                                ClickHouseMetadataService clickHouseMetadataService) {
        this.dbConnectionService = dbConnectionService;
        this.clickHouseMetadataService = clickHouseMetadataService;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("ch/databases")
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
        List<ChDatabaseInfo> all = new ArrayList<>(clickHouseMetadataService.listDatabaseInfos(id));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        if ("size".equals(sortBy)) {
            all.sort(desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk()));
        } else {
            all.sort(desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        }
        int limit = Math.min(Math.max(1, size), 500);
        int offset = Math.max(0, (page - 1) * limit);
        List<ChDatabaseInfo> dbs = offset < all.size() ? all.subList(offset, Math.min(offset + limit, all.size())) : List.of();
        model.put("items", dbs);
        model.put("itemType", "database");
        model.put("itemUrlPrefix", "/ch/" + id + "/");
        model.put("page", page);
        model.put("size", limit);
        model.put("sort", sortBy);
        model.put("order", order != null ? order : "asc");
        model.put("fromRow", all.isEmpty() ? 0 : offset + 1);
        model.put("toRow", offset + dbs.size());
        model.put("hasPrev", page > 1);
        model.put("hasMore", offset + dbs.size() < all.size());
        model.put("prevOffset", Math.max(0, offset - limit));
        model.put("nextOffset", offset + limit);

        return model;
    }

    @Get("/{id}/detail")
    public HttpResponse<?> detailRedirect(@PathVariable Long id) {
        return HttpResponse.redirect(java.net.URI.create("/ch/" + id));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}")
    @View("ch/tables")
    public Map<String, Object> tables(@PathVariable Long id, @PathVariable String dbName,
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
        List<ChTableInfo> all = new ArrayList<>(clickHouseMetadataService.listTableInfos(id, dbName));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        if ("type".equalsIgnoreCase(sortBy)) {
            all.sort(desc ? (a, b) -> (b.getType() != null ? b.getType() : "").compareToIgnoreCase(a.getType() != null ? a.getType() : "") : (a, b) -> (a.getType() != null ? a.getType() : "").compareToIgnoreCase(b.getType() != null ? b.getType() : ""));
        } else if ("size".equals(sortBy)) {
            all.sort(desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk()));
        } else if ("rows".equals(sortBy)) {
            all.sort(desc ? (a, b) -> Long.compare(b.getApproximateRowCount(), a.getApproximateRowCount()) : (a, b) -> Long.compare(a.getApproximateRowCount(), b.getApproximateRowCount()));
        } else {
            all.sort(desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        }
        int offset = Math.max(0, (page - 1) * limit);
        List<ChTableInfo> tables = offset < all.size() ? all.subList(offset, Math.min(offset + limit, all.size())) : List.of();
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/ch/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("tables", tables);
        model.put("page", page);
        model.put("size", limit);
        model.put("sort", sortBy);
        model.put("order", order != null ? order : "asc");
        model.put("fromRow", all.isEmpty() ? 0 : offset + 1);
        model.put("toRow", offset + tables.size());
        model.put("hasPrev", page > 1);
        model.put("hasMore", offset + tables.size() < all.size());
        model.put("prevOffset", Math.max(0, offset - limit));
        model.put("nextOffset", offset + limit);

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/sql")
    @View("ch/sql")
    public Map<String, Object> sqlPageGet(@PathVariable Long id, @PathVariable String dbName,
                                         @QueryValue(value = "sql", defaultValue = "") String sql,
                                         @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                         @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                         @QueryValue(value = "sort", defaultValue = "") String sort,
                                         @QueryValue(value = "order", defaultValue = "") String order) {
        return buildSqlPageModel(id, dbName, sql, offset, limit, sort, order);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/sql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("ch/sql")
    public Map<String, Object> sqlPagePost(@PathVariable Long id, @PathVariable String dbName,
                                           String sql, @Nullable Integer offset, @Nullable Integer limit,
                                           @Nullable String sort, @Nullable String order) {
        return buildSqlPageModel(id, dbName, sql, offset, limit, sort, order);
    }

    private Map<String, Object> buildSqlPageModel(Long id, String dbName, String sql,
                                                  Integer offset, Integer limit, String sort, String order) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/ch/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/ch/" + id + "/" + dbName));
        breadcrumbs.add(new BreadcrumbItem("sql", null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
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
            var result = clickHouseMetadataService.executeQuery(id, dbName, sql, off, lim, sort, order)
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
    public Object executeQuery(@PathVariable Long id, String sql, String dbName,
                              @Nullable Integer offset, @Nullable Integer limit,
                              @Nullable String sort, @Nullable String order, String target) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("dbName", dbName);
        if (sql == null || sql.isBlank()) {
            model.put("error", "Empty query");
            return "table".equals(target)
                    ? new ModelAndView<>("partials/ch-table-view-result", model)
                    : new ModelAndView<>("partials/ch-query-result", model);
        }

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = clickHouseMetadataService.executeQuery(id, dbName, sql, off, lim, sort, order)
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
                ? new ModelAndView<>("partials/ch-table-view-result", model)
                : new ModelAndView<>("partials/ch-query-result", model);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("ch/detail")
    public Map<String, Object> rowDetail(@PathVariable Long id, @PathVariable String dbName,
                                        String sql, Integer rowNum, String sort, String order) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/ch/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName != null ? dbName : "", "/ch/" + id + "/" + (dbName != null ? dbName : "")));
        breadcrumbs.add(new BreadcrumbItem("detail", null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName != null ? dbName : "");

        List<Map<String, String>> detailRows = new ArrayList<>();
        if (sql != null && !sql.isBlank() && rowNum != null && rowNum >= 0) {
            int offset = Math.max(0, rowNum);
            var result = clickHouseMetadataService.executeQuery(id, dbName, sql, offset, 1, sort, order, false);
            if (result.isPresent() && !result.get().hasError() && result.get().getRows() != null && !result.get().getRows().isEmpty()) {
                List<String> columns = result.get().getColumns();
                List<String> columnTypes = result.get().getColumnTypes() != null ? result.get().getColumnTypes() : List.of();
                List<Object> row = result.get().getRows().get(0);
                for (int i = 0; i < columns.size(); i++) {
                    String col = columns.get(i);
                    String type = i < columnTypes.size() ? columnTypes.get(i) : "unknown";
                    Object val = i < row.size() ? row.get(i) : null;
                    Map<String, String> entry = new HashMap<>();
                    entry.put("name", col);
                    entry.put("type", type);
                    entry.put("value", val != null ? val.toString() : "");
                    detailRows.add(entry);
                }
            }
        }
        model.put("detailRows", detailRows);

        return model;
    }

    private Map<String, Object> baseModel(Long id) {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        dbConnectionService.findById(id).ifPresent(conn -> model.put("connection", conn));

        return model;
    }
}
