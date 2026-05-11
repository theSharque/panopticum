package com.panopticum.lightjdbc.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.ui.AppAlerts;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.lightjdbc.service.LightJdbcMetadataService;
import io.micronaut.context.annotation.Value;
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
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/lightjdbc")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class LightJdbcController {

    private final DbConnectionService dbConnectionService;
    private final LightJdbcMetadataService lightJdbcMetadataService;

    @Value("${panopticum.read-only:false}")
    private boolean readOnly;

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/schemas")
    @View("lightjdbc/schemas")
    public Map<String, Object> schemas(@PathVariable Long id,
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
        model.put("itemType", "schema");
        model.put("itemUrlPrefix", "/lightjdbc/" + id + "/");

        Page<SchemaInfo> paged = lightJdbcMetadataService.listSchemasPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(), Map.of("name", "orderName"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{schema}/tables")
    @View("lightjdbc/tables")
    public Map<String, Object> tables(@PathVariable Long id, @PathVariable String schema,
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
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/lightjdbc/" + id + "/schemas"));
        breadcrumbs.add(new BreadcrumbItem(schema, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("schemaName", schema);

        Page<TableInfo> paged = lightJdbcMetadataService.listTablesPaged(id, schema, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "tables");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "type", "orderType"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{schema}/sql")
    @View("lightjdbc/sql")
    public Map<String, Object> sqlPageGet(@PathVariable Long id, @PathVariable String schema,
                                          @QueryValue(value = "sql", defaultValue = "") String sql,
                                          @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                          @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                          @QueryValue(value = "sort", defaultValue = "") String sort,
                                          @QueryValue(value = "order", defaultValue = "") String order,
                                          @QueryValue(value = "search", defaultValue = "") String search) {
        return buildSqlPageModel(id, schema, sql, offset, limit, sort, order, search);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{schema}/sql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("lightjdbc/sql")
    public Map<String, Object> sqlPagePost(@PathVariable Long id, @PathVariable String schema,
                                           String sql, @Nullable Integer offset, @Nullable Integer limit,
                                           @Nullable String sort, @Nullable String order, @Nullable String search) {
        return buildSqlPageModel(id, schema, sql, offset, limit, sort, order, search);
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{schema}/{table}")
    @View("lightjdbc/table")
    public Map<String, Object> tablePage(@PathVariable Long id, @PathVariable String schema, @PathVariable String table,
                                         @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                         @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                         @QueryValue(value = "sort", defaultValue = "") String sort,
                                         @QueryValue(value = "order", defaultValue = "") String order,
                                         @QueryValue(value = "search", defaultValue = "") String search) {
        String tableClean = table != null ? table.replace("\"", "").trim() : "";
        String quoted = quoteTable(schema, tableClean);
        String sql = "SELECT * FROM " + quoted;
        Map<String, Object> model = buildSqlPageModel(id, schema, sql, offset, limit, sort, order, search);
        @SuppressWarnings("unchecked")
        List<BreadcrumbItem> breadcrumbs = (List<BreadcrumbItem>) model.get("breadcrumbs");
        if (breadcrumbs != null && !breadcrumbs.isEmpty()) {
            breadcrumbs.set(breadcrumbs.size() - 1, new BreadcrumbItem(tableClean, null));
        }
        ControllerModelHelper.refreshBreadcrumbPath(model);

        return model;
    }

    private static String quoteTable(String schema, String table) {
        String s = schema != null ? schema.replace("\"", "\"\"") : "";
        String t = table != null ? table.replace("\"", "\"\"") : "";
        return "\"" + s + "\".\"" + t + "\"";
    }

    private Map<String, Object> buildSqlPageModel(Long id, String schema, String sql,
                                                  Integer offset, Integer limit, String sort, String order, String search) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/lightjdbc/" + id + "/schemas"));
        breadcrumbs.add(new BreadcrumbItem(schema, "/lightjdbc/" + id + "/" + schema + "/tables"));
        breadcrumbs.add(new BreadcrumbItem("sql", null, false));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("schemaName", schema);
        model.put("sql", sql != null ? sql : "");
        model.put("tableQueryActionUrl", "/lightjdbc/" + id + "/query");
        String tableSeg = lightJdbcMetadataService.parseTableFromSql(sql != null ? sql : "").orElse(null);
        model.put("tableDetailActionUrl", tableSeg != null && !tableSeg.isBlank()
                ? "/lightjdbc/" + id + "/" + schema + "/" + simplifyTableSegment(tableSeg) + "/detail"
                : "/lightjdbc/" + id + "/" + schema + "/detail");
        model.put("includeAlertOob", false);

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? Math.min(limit, 1000) : 100;
        model.put("size", lim);

        if (sql == null || sql.isBlank()) {
            AppAlerts.clear(model);
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
            var result = lightJdbcMetadataService.executeQuery(id, sql, off, lim, sort, order, searchTerm)
                    .orElse(QueryResult.error("error.queryExecutionFailed"));
            AppAlerts.fromControllerMessage(model, result.hasError() ? result.getError() : null);
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

    private static String simplifyTableSegment(String qualified) {
        int dot = qualified.lastIndexOf('.');
        if (dot >= 0 && dot < qualified.length() - 1) {
            return qualified.substring(dot + 1).replace("\"", "").trim();
        }
        return qualified.replace("\"", "").trim();
    }

    @Post("/{id}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object executeQuery(@PathVariable Long id, String sql, String schemaName,
                               @Nullable Integer offset, @Nullable Integer limit,
                               @Nullable String sort, @Nullable String order, @Nullable String search, String target) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("schemaName", schemaName);
        model.put("includeAlertOob", true);
        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);
        if (sql == null || sql.isBlank()) {
            AppAlerts.raw(model, "Empty query");
            model.put("queryActionUrl", "/lightjdbc/" + id + "/query");
            model.put("tableQueryActionUrl", "/lightjdbc/" + id + "/query");
            model.put("tableDetailActionUrl", "/lightjdbc/" + id + "/" + schemaName + "/detail");

            return "table".equals(target)
                    ? new ModelAndView<>("partials/table-view-result", model)
                    : new ModelAndView<>("partials/query-result", model);
        }
        model.put("queryActionUrl", "/lightjdbc/" + id + "/query");
        model.put("tableQueryActionUrl", "/lightjdbc/" + id + "/query");
        String tableSegment = lightJdbcMetadataService.parseTableFromSql(sql).orElse(null);
        model.put("tableDetailActionUrl", tableSegment != null && !tableSegment.isBlank()
                ? "/lightjdbc/" + id + "/" + schemaName + "/" + simplifyTableSegment(tableSegment) + "/detail"
                : "/lightjdbc/" + id + "/" + schemaName + "/detail");

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = lightJdbcMetadataService.executeQuery(id, sql, off, lim, sort, order, searchTerm)
                .orElse(QueryResult.error("Execution failed"));
        AppAlerts.fromControllerMessage(model, result.hasError() ? result.getError() : null);
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
    @Post("/{id}/{schema}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("lightjdbc/detail")
    public Map<String, Object> rowDetail(@PathVariable Long id, @PathVariable String schema,
                                         String sql, Integer rowNum, String sort, String order, @Nullable String search) {
        return rowDetail(id, schema, sql, rowNum, sort, order, search, null);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{schema}/{table}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("lightjdbc/detail")
    public Map<String, Object> rowDetailWithTable(@PathVariable Long id, @PathVariable String schema, @PathVariable String table,
                                                  String sql, Integer rowNum, String sort, String order, @Nullable String search) {
        return rowDetail(id, schema, sql, rowNum, sort, order, search, table);
    }

    private Map<String, Object> rowDetail(Long id, String schema, String sql, Integer rowNum,
                                          String sort, String order, String search, @Nullable String tableParam) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/lightjdbc/" + id + "/schemas"));
        breadcrumbs.add(new BreadcrumbItem(schema, "/lightjdbc/" + id + "/" + schema + "/tables"));
        if (tableParam != null && !tableParam.isBlank()) {
            breadcrumbs.add(new BreadcrumbItem(tableParam, "/lightjdbc/" + id + "/" + schema + "/" + tableParam));
        }
        breadcrumbs.add(new BreadcrumbItem("detail", null, false));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("schemaName", schema);
        model.put("sql", sql != null ? sql : "");
        model.put("rowNum", rowNum != null ? rowNum : 0);
        model.put("sort", sort != null ? sort : "");
        model.put("order", order != null ? order : "");
        model.put("searchTerm", search != null && !search.isBlank() ? search.trim() : "");

        if (sql != null && !sql.isBlank() && rowNum != null && rowNum >= 0) {
            Map<String, Object> detailResult = lightJdbcMetadataService.getDetailRow(id, schema, sql, Math.max(0, rowNum), sort, order);
            model.putAll(detailResult);
        } else {
            model.put("detailRows", List.<Map<String, String>>of());
            model.put("editable", false);
        }
        model.put("readOnly", readOnly);

        return model;
    }
}
