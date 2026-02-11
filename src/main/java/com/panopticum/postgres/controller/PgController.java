package com.panopticum.postgres.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.postgres.service.PgMetadataService;
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

@Controller("/pg")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class PgController {

    private final DbConnectionService dbConnectionService;
    private final PgMetadataService pgMetadataService;

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("pg/databases")
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
        model.put("schema", null);
        model.put("itemType", "database");
        model.put("itemUrlPrefix", "/pg/" + id + "/");

        Page<DatabaseInfo> paged = pgMetadataService.listDatabasesPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "size", "orderSize"));

        return model;
    }

    @Get("/{id}/detail")
    public HttpResponse<?> detailRedirect(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/pg/" + id));
    }

    @Get("/{id}/{dbName}/{schema}/detail")
    public HttpResponse<?> detailRedirectWithContext(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema) {
        return HttpResponse.redirect(URI.create("/pg/" + id + "/" + dbName + "/" + schema));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}")
    @View("pg/schemas")
    public Map<String, Object> schemas(@PathVariable Long id, @PathVariable String dbName,
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
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/pg/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("schema", null);
        model.put("itemType", "schema");
        model.put("itemUrlPrefix", "/pg/" + id + "/" + dbName + "/");

        Page<SchemaInfo> paged = pgMetadataService.listSchemasPaged(id, dbName, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "owner", "orderOwner", "tables", "orderTables"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/{schema}")
    @View("pg/tables")
    public Map<String, Object> tables(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
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
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/pg/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/pg/" + id + "/" + dbName));
        breadcrumbs.add(new BreadcrumbItem(schema, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("schema", schema);

        Page<TableInfo> paged = pgMetadataService.listTablesPaged(id, dbName, schema, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "tables");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "type", "orderType", "rows", "orderRows", "size", "orderSize"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/{schema}/sql")
    @View("pg/sql")
    public Map<String, Object> sqlPageGet(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                         @QueryValue(value = "sql", defaultValue = "") String sql,
                                         @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                         @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                         @QueryValue(value = "sort", defaultValue = "") String sort,
                                         @QueryValue(value = "order", defaultValue = "") String order,
                                         @QueryValue(value = "search", defaultValue = "") String search) {
        return buildSqlPageModel(id, dbName, schema, sql, offset, limit, sort, order, search);
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/{schema}/{table}")
    @View("pg/table")
    public Map<String, Object> tablePage(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                         @PathVariable String table,
                                         @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                         @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                         @QueryValue(value = "sort", defaultValue = "") String sort,
                                         @QueryValue(value = "order", defaultValue = "") String order,
                                         @QueryValue(value = "search", defaultValue = "") String search) {
        String sql = "SELECT * FROM \"" + schema.replace("\"", "\"\"") + "\".\"" + table.replace("\"", "\"\"") + "\"";
        return buildSqlPageModel(id, dbName, schema, sql, offset, limit, sort, order, search);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/{schema}/sql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("pg/sql")
    public Map<String, Object> sqlPagePost(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                          String sql, @Nullable Integer offset, @Nullable Integer limit,
                                          @Nullable String sort, @Nullable String order, @Nullable String search) {
        return buildSqlPageModel(id, dbName, schema, sql, offset, limit, sort, order, search);
    }

    private Map<String, Object> buildSqlPageModel(Long id, String dbName, String schema, String sql,
                                                  Integer offset, Integer limit, String sort, String order, String search) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);

        Optional<String> qualifiedTable = pgMetadataService.parseTableFromSql(sql);
        String tableLabel = qualifiedTable
                .map(PgController::simpleTableName)
                .orElse("sql");

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/pg/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/pg/" + id + "/" + dbName));
        breadcrumbs.add(new BreadcrumbItem(schema, "/pg/" + id + "/" + dbName + "/" + schema));
        breadcrumbs.add(new BreadcrumbItem(tableLabel, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("schema", schema);
        model.put("sql", sql != null ? sql : "");
        model.put("tableQueryActionUrl", "/pg/" + id + "/query");
        String tableSegment = qualifiedTable
                .map(PgController::simpleTableName)
                .orElse("sql");
        model.put("tableDetailActionUrl", "/pg/" + id + "/" + dbName + "/" + schema + "/" + tableSegment + "/detail");

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
            var result = pgMetadataService.executeQuery(id, dbName, sql, off, lim, sort, order, searchTerm)
                    .orElse(QueryResult.error("error.queryExecutionFailed"));
            putQueryResultIntoModel(model, result, sql != null ? sql : "", sort, order);
        }

        return model;
    }

    private void putQueryResultIntoModel(Map<String, Object> model, QueryResult result, String sql,
                                        String sort, String order) {
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
        if (!model.containsKey("searchTerm")) {
            model.put("searchTerm", "");
        }
    }

    @Post("/{id}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object executeQuery(@PathVariable Long id, String sql, String dbName, String schema,
                              @Nullable Integer offset, @Nullable Integer limit,
                              @Nullable String sort, @Nullable String order, @Nullable String search, String target) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("schema", schema);
        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);

        if (sql == null || sql.isBlank()) {
            model.put("error", "Empty query");
            model.put("queryActionUrl", "/pg/" + id + "/query");
            model.put("tableQueryActionUrl", "/pg/" + id + "/query");
            model.put("tableDetailActionUrl", "/pg/" + id + "/" + dbName + "/" + schema + "/detail");

            return "table".equals(target)
                    ? new ModelAndView<>("partials/table-view-result", model)
                    : new ModelAndView<>("partials/query-result", model);
        }
        model.put("queryActionUrl", "/pg/" + id + "/query");
        model.put("tableQueryActionUrl", "/pg/" + id + "/query");
        String tableSegment = pgMetadataService.parseTableFromSql(sql).map(PgController::simpleTableName).orElse(null);
        model.put("tableDetailActionUrl", tableSegment != null && !tableSegment.isBlank()
                ? "/pg/" + id + "/" + dbName + "/" + schema + "/" + tableSegment + "/detail"
                : "/pg/" + id + "/" + dbName + "/" + schema + "/detail");

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = pgMetadataService.executeQuery(id, dbName, sql, off, lim, sort, order, searchTerm)
                .orElse(QueryResult.error("Execution failed"));
        putQueryResultIntoModel(model, result, sql, sort, order);
        model.put("sql", sql);

        return "table".equals(target)
                ? new ModelAndView<>("partials/table-view-result", model)
                : new ModelAndView<>("partials/query-result", model);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/{schema}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("pg/detail")
    public Map<String, Object> rowDetail(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                         String sql, Integer rowNum, String sort, String order, @Nullable String search) {
        return rowDetail(id, dbName, schema, sql, rowNum, sort, order, search, null);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/{schema}/{table}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("pg/detail")
    public Map<String, Object> rowDetailWithTable(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                                  @PathVariable String table, String sql, Integer rowNum, String sort, String order,
                                                  @Nullable String search) {
        return rowDetail(id, dbName, schema, sql, rowNum, sort, order, search, table);
    }

    private Map<String, Object> rowDetail(Long id, String dbName, String schema, String sql, Integer rowNum,
                                          String sort, String order, String search, @Nullable String tableParam) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String tableLabel = tableParam;
        if (tableLabel == null || tableLabel.isBlank()) {
            tableLabel = pgMetadataService.parseTableFromSql(sql != null ? sql : "").map(PgController::simpleTableName).orElse(null);
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/pg/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName != null ? dbName : "", "/pg/" + id + "/" + (dbName != null ? dbName : "")));
        String schemaUrl = (dbName != null && schema != null && !dbName.isBlank() && !schema.isBlank())
                ? "/pg/" + id + "/" + dbName + "/" + schema
                : null;
        breadcrumbs.add(new BreadcrumbItem(schema != null ? schema : "", schemaUrl));
        if (tableLabel != null && !tableLabel.isBlank()) {
            breadcrumbs.add(new BreadcrumbItem(tableLabel, "/pg/" + id + "/" + dbName + "/" + schema + "/" + tableLabel));
        }
        breadcrumbs.add(new BreadcrumbItem("detail", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName != null ? dbName : "");
        model.put("schema", schema != null ? schema : "");
        model.put("sql", sql != null ? sql : "");
        model.put("rowNum", rowNum != null ? rowNum : 0);
        model.put("sort", sort != null ? sort : "");
        model.put("order", order != null ? order : "");
        model.put("searchTerm", search != null && !search.isBlank() ? search.trim() : "");
        if (tableLabel != null && !tableLabel.isBlank()) {
            model.put("table", tableLabel);
        }

        List<Map<String, String>> detailRows = new ArrayList<>();
        String rowCtid = null;
        if (sql != null && !sql.isBlank() && rowNum != null && rowNum >= 0) {
            var result = pgMetadataService.getDetailRowWithCtid(id, dbName, schema, sql, Math.max(0, rowNum), sort, order);
            if (result.containsKey("error")) {
                model.put("error", result.get("error"));
            } else {
                @SuppressWarnings("unchecked")
                List<Map<String, String>> rows = (List<Map<String, String>>) result.get("detailRows");
                if (rows != null) {
                    detailRows = rows;
                }
                rowCtid = (String) result.get("rowCtid");
            }
        }

        model.put("detailRows", detailRows);
        model.put("rowCtid", rowCtid != null ? rowCtid : "");

        return model;
    }

    @Post("/{id}/{dbName}/{schema}/detail/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveRow(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                         @Body Map<String, String> form) {
        return saveRow(id, dbName, schema, form, null);
    }

    @Post("/{id}/{dbName}/{schema}/{table}/detail/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveRowWithTable(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                   @PathVariable String table, @Body Map<String, String> form) {
        return saveRow(id, dbName, schema, form, table);
    }

    private Object saveRow(Long id, String dbName, String schema, Map<String, String> form, @Nullable String tableParam) {
        String sql = form != null ? form.get("sql") : null;
        Integer rowNum = form != null && form.containsKey("rowNum") ? parseInteger(form.get("rowNum")) : null;
        String sort = form != null ? form.get("sort") : null;
        String order = form != null ? form.get("order") : null;
        String ctid = form != null ? form.get("ctid") : null;
        Map<String, String> columnValues = new LinkedHashMap<>();
        if (form != null) {
            for (Map.Entry<String, String> e : form.entrySet()) {
                if (e.getKey() != null && e.getKey().startsWith("field_")) {
                    columnValues.put(e.getKey().substring(6), e.getValue() != null ? e.getValue() : "");
                }
            }
        }

        String searchParam = form != null ? form.get("search") : null;
        Optional<String> qualifiedTable = pgMetadataService.parseTableFromSql(sql);
        if (qualifiedTable.isEmpty()) {
            Map<String, Object> model = rowDetail(id, dbName, schema, sql, rowNum, sort, order, searchParam, tableParam);
            model.put("error", "Could not determine table from SQL.");

            return new ModelAndView<>("pg/detail", model);
        }

        Optional<String> err = pgMetadataService.executeUpdateByCtid(id, dbName, qualifiedTable.get(), ctid, columnValues);
        if (err.isPresent()) {
            Map<String, Object> model = rowDetail(id, dbName, schema, sql, rowNum, sort, order, searchParam, tableParam);
            model.put("error", err.get());

            return new ModelAndView<>("pg/detail", model);
        }

        return new ModelAndView<>("pg/detail", rowDetail(id, dbName, schema, sql, rowNum, sort, order, searchParam, tableParam));
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

    private static String simpleTableName(String qualified) {
        if (qualified == null || qualified.isBlank()) {
            return "";
        }
        String trimmed = qualified.trim();
        int dot = trimmed.lastIndexOf('.');
        return dot >= 0 && dot + 1 < trimmed.length() ? trimmed.substring(dot + 1) : trimmed;
    }
}
