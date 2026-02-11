package com.panopticum.mysql.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.mysql.service.MySqlMetadataService;
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

@Controller("/mysql")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class MySqlController {

    private final DbConnectionService dbConnectionService;
    private final MySqlMetadataService mySqlMetadataService;

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("mysql/databases")
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
        model.put("itemUrlPrefix", "/mysql/" + id + "/");

        Page<DatabaseInfo> paged = mySqlMetadataService.listDatabasesPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "size", "orderSize"));

        return model;
    }

    @Get("/{id}/detail")
    public HttpResponse<?> detailRedirect(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/mysql/" + id));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}")
    @View("mysql/tables")
    public Map<String, Object> tables(@PathVariable Long id, @PathVariable String dbName,
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
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mysql/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);

        Page<TableInfo> paged = mySqlMetadataService.listTablesPaged(id, dbName, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "tables");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "type", "orderType", "rows", "orderRows", "size", "orderSize"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/sql")
    @View("mysql/sql")
    public Map<String, Object> sqlPageGet(@PathVariable Long id, @PathVariable String dbName,
                                          @QueryValue(value = "sql", defaultValue = "") String sql,
                                          @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                          @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                          @QueryValue(value = "sort", defaultValue = "") String sort,
                                          @QueryValue(value = "order", defaultValue = "") String order,
                                          @QueryValue(value = "search", defaultValue = "") String search) {
        return buildSqlPageModel(id, dbName, sql, offset, limit, sort, order, search);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/sql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("mysql/sql")
    public Map<String, Object> sqlPagePost(@PathVariable Long id, @PathVariable String dbName,
                                           String sql, @Nullable Integer offset, @Nullable Integer limit,
                                           @Nullable String sort, @Nullable String order, @Nullable String search) {
        return buildSqlPageModel(id, dbName, sql, offset, limit, sort, order, search);
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/{table}")
    @View("mysql/table")
    public Map<String, Object> tablePage(@PathVariable Long id, @PathVariable String dbName, @PathVariable String table,
                                         @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                         @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                         @QueryValue(value = "sort", defaultValue = "") String sort,
                                         @QueryValue(value = "order", defaultValue = "") String order,
                                         @QueryValue(value = "search", defaultValue = "") String search) {
        String tableEscaped = table != null ? table.replace("`", "``") : "";
        String sql = "SELECT * FROM `" + tableEscaped + "`";
        Map<String, Object> model = buildSqlPageModel(id, dbName, sql, offset, limit, sort, order, search);
        @SuppressWarnings("unchecked")
        List<BreadcrumbItem> breadcrumbs = (List<BreadcrumbItem>) model.get("breadcrumbs");
        if (breadcrumbs != null && !breadcrumbs.isEmpty()) {
            breadcrumbs.set(breadcrumbs.size() - 1, new BreadcrumbItem(table, null));
        }
        return model;
    }

    private Map<String, Object> buildSqlPageModel(Long id, String dbName, String sql,
                                                  Integer offset, Integer limit, String sort, String order, String search) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mysql/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/mysql/" + id + "/" + dbName));
        breadcrumbs.add(new BreadcrumbItem("sql", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("sql", sql != null ? sql : "");
        model.put("tableQueryActionUrl", "/mysql/" + id + "/query");
        String tableSegment = mySqlMetadataService.parseTableFromSql(sql != null ? sql : "").map(MySqlController::simpleTableName).orElse(null);
        model.put("tableDetailActionUrl", tableSegment != null && !tableSegment.isBlank()
                ? "/mysql/" + id + "/" + dbName + "/" + tableSegment + "/detail"
                : "/mysql/" + id + "/" + dbName + "/detail");

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
            var result = mySqlMetadataService.executeQuery(id, dbName, sql, off, lim, sort, order, searchTerm)
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
                               @Nullable String sort, @Nullable String order, @Nullable String search, String target) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("dbName", dbName);
        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);
        if (sql == null || sql.isBlank()) {
            model.put("error", "Empty query");
            model.put("queryActionUrl", "/mysql/" + id + "/query");
            model.put("tableQueryActionUrl", "/mysql/" + id + "/query");
            model.put("tableDetailActionUrl", "/mysql/" + id + "/" + dbName + "/detail");

            return "table".equals(target)
                    ? new ModelAndView<>("partials/table-view-result", model)
                    : new ModelAndView<>("partials/query-result", model);
        }
        model.put("queryActionUrl", "/mysql/" + id + "/query");
        model.put("tableQueryActionUrl", "/mysql/" + id + "/query");
        String tableSegment = mySqlMetadataService.parseTableFromSql(sql).map(MySqlController::simpleTableName).orElse(null);
        model.put("tableDetailActionUrl", tableSegment != null && !tableSegment.isBlank()
                ? "/mysql/" + id + "/" + dbName + "/" + tableSegment + "/detail"
                : "/mysql/" + id + "/" + dbName + "/detail");

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = mySqlMetadataService.executeQuery(id, dbName, sql, off, lim, sort, order, searchTerm)
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
                ? new ModelAndView<>("partials/table-view-result", model)
                : new ModelAndView<>("partials/query-result", model);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("mysql/detail")
    public Map<String, Object> rowDetail(@PathVariable Long id, @PathVariable String dbName,
                                         String sql, Integer rowNum, String sort, String order, @Nullable String search) {
        return rowDetail(id, dbName, sql, rowNum, sort, order, search, null);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/{table}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("mysql/detail")
    public Map<String, Object> rowDetailWithTable(@PathVariable Long id, @PathVariable String dbName, @PathVariable String table,
                                                   String sql, Integer rowNum, String sort, String order, @Nullable String search) {
        return rowDetail(id, dbName, sql, rowNum, sort, order, search, table);
    }

    private Map<String, Object> rowDetail(Long id, String dbName, String sql, Integer rowNum,
                                          String sort, String order, String search, @Nullable String tableParam) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String tableLabel = tableParam;
        if (tableLabel == null || tableLabel.isBlank()) {
            tableLabel = mySqlMetadataService.parseTableFromSql(sql != null ? sql : "").map(MySqlController::simpleTableName).orElse(null);
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/mysql/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName != null ? dbName : "", "/mysql/" + id + "/" + (dbName != null ? dbName : "")));
        if (tableLabel != null && !tableLabel.isBlank()) {
            breadcrumbs.add(new BreadcrumbItem(tableLabel, "/mysql/" + id + "/" + dbName + "/" + tableLabel));
        }
        breadcrumbs.add(new BreadcrumbItem("detail", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName != null ? dbName : "");
        model.put("sql", sql != null ? sql : "");
        model.put("rowNum", rowNum != null ? rowNum : 0);
        model.put("sort", sort != null ? sort : "");
        model.put("order", order != null ? order : "");
        model.put("searchTerm", search != null && !search.isBlank() ? search.trim() : "");
        if (tableLabel != null && !tableLabel.isBlank()) {
            model.put("table", tableLabel);
        }

        if (sql != null && !sql.isBlank() && rowNum != null && rowNum >= 0) {
            Map<String, Object> detailResult = mySqlMetadataService.getDetailRow(id, dbName, sql, Math.max(0, rowNum), sort, order);
            model.putAll(detailResult);
        } else {
            model.put("detailRows", List.<Map<String, String>>of());
            model.put("editable", false);
        }

        return model;
    }

    @Post("/{id}/{dbName}/detail/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveRow(@PathVariable Long id, @PathVariable String dbName,
                         @Body Map<String, String> form) {
        return saveRow(id, dbName, form, null);
    }

    @Post("/{id}/{dbName}/{table}/detail/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveRowWithTable(@PathVariable Long id, @PathVariable String dbName, @PathVariable String table,
                                   @Body Map<String, String> form) {
        return saveRow(id, dbName, form, table);
    }

    private Object saveRow(Long id, String dbName, Map<String, String> form, @Nullable String tableParam) {
        String sql = form != null ? form.get("sql") : null;
        Integer rowNum = form != null && form.containsKey("rowNum") ? parseInteger(form.get("rowNum")) : null;
        String sort = form != null ? form.get("sort") : null;
        String order = form != null ? form.get("order") : null;
        String qualifiedTable = form != null ? form.get("qualifiedTable") : null;

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

        String searchParam = form != null ? form.get("search") : null;
        Optional<String> parsedTable = mySqlMetadataService.parseTableFromSql(sql);
        if (parsedTable.isEmpty()) {
            Map<String, Object> model = rowDetail(id, dbName, sql, rowNum, sort, order, searchParam, tableParam);
            model.put("error", "Could not determine table from SQL.");
            return new ModelAndView<>("mysql/detail", model);
        }

        String tableRef = qualifiedTable != null && !qualifiedTable.isBlank() ? qualifiedTable : parsedTable.get();
        Optional<String> err = mySqlMetadataService.executeUpdateByKey(id, dbName, tableRef, uniqueKeyColumns, keyValues, columnValues);
        if (err.isPresent()) {
            Map<String, Object> model = rowDetail(id, dbName, sql, rowNum, sort, order, searchParam, tableParam);
            model.put("error", err.get());
            return new ModelAndView<>("mysql/detail", model);
        }

        return new ModelAndView<>("mysql/detail", rowDetail(id, dbName, sql, rowNum, sort, order, searchParam, tableParam));
    }

    private static String simpleTableName(String qualified) {
        if (qualified == null || qualified.isBlank()) {
            return "";
        }
        String trimmed = qualified.trim();
        int dot = trimmed.lastIndexOf('.');
        if (dot >= 0 && dot + 1 < trimmed.length()) {
            String part = trimmed.substring(dot + 1).replace("`", "").trim();
            return part.isEmpty() ? trimmed.replace("`", "").trim() : part;
        }
        return trimmed.replace("`", "").trim();
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
}
