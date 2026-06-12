package com.panopticum.sqlserver.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.controller.AbstractConnectionUiController;
import com.panopticum.core.error.ErrorKeys;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.ui.AppAlerts;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.core.util.QueryResultModelHelper;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.sqlserver.service.SqlServerMetadataService;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/sqlserver")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
public class SqlServerController extends AbstractConnectionUiController {

    private final SqlServerMetadataService sqlServerMetadataService;
    private final ObjectMapper objectMapper;

    public SqlServerController(DbConnectionService dbConnectionService,
                               SqlServerMetadataService sqlServerMetadataService,
                               ObjectMapper objectMapper) {
        super(dbConnectionService);
        this.sqlServerMetadataService = sqlServerMetadataService;
        this.objectMapper = objectMapper;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("sqlserver/databases")
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
        model.put("itemUrlPrefix", "/sqlserver/" + id + "/");

        Page<DatabaseInfo> paged = sqlServerMetadataService.listDatabasesPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "size", "orderSize"));

        return model;
    }

    @Get("/{id}/detail")
    public HttpResponse<?> detailRedirect(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/sqlserver/" + id));
    }

    @Get("/{id}/{dbName}/{schema}/detail")
    public HttpResponse<?> detailRedirectWithContext(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema) {
        return HttpResponse.redirect(URI.create("/sqlserver/" + id + "/" + dbName + "/" + schema));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}")
    @View("sqlserver/schemas")
    public Map<String, Object> schemas(@PathVariable Long id, @PathVariable String dbName,
                                      @QueryValue(value = "page", defaultValue = "1") int page,
                                      @QueryValue(value = "size", defaultValue = "50") int size,
                                      @QueryValue(value = "sort", defaultValue = "name") String sort,
                                      @QueryValue(value = "order", defaultValue = "asc") String order) {
        String dbNameClean = unquoteBracket(dbName);
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/sqlserver/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbNameClean != null ? dbNameClean : "", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbNameClean != null ? dbNameClean : "");
        model.put("schema", null);
        model.put("itemType", "schema");
        model.put("itemUrlPrefix", "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "") + "/");

        Page<SchemaInfo> paged = sqlServerMetadataService.listSchemasPaged(id, dbNameClean != null ? dbNameClean : "", page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "owner", "orderOwner", "tables", "orderTables"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/{schema}")
    @View("sqlserver/tables")
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

        String dbNameClean = unquoteBracket(dbName);
        String schemaClean = unquoteBracket(schema);
        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/sqlserver/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbNameClean != null ? dbNameClean : "", "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "")));
        breadcrumbs.add(new BreadcrumbItem(schemaClean != null ? schemaClean : "", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbNameClean != null ? dbNameClean : "");
        model.put("schema", schemaClean != null ? schemaClean : "");

        Page<TableInfo> paged = sqlServerMetadataService.listTablesPaged(id, dbNameClean != null ? dbNameClean : "", schemaClean != null ? schemaClean : "", page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "tables");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "type", "orderType", "rows", "orderRows", "size", "orderSize"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/{schema}/sql")
    @View("sqlserver/sql")
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
    @Post("/{id}/{dbName}/{schema}/sql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("sqlserver/sql")
    public Map<String, Object> sqlPagePost(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                           String sql, @Nullable Integer offset, @Nullable Integer limit,
                                           @Nullable String sort, @Nullable String order, @Nullable String search) {
        return buildSqlPageModel(id, dbName, schema, sql, offset, limit, sort, order, search);
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/{schema}/{table}")
    @View("sqlserver/table")
    public Map<String, Object> tablePage(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                        @PathVariable String table,
                                        @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                        @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                        @QueryValue(value = "sort", defaultValue = "") String sort,
                                        @QueryValue(value = "order", defaultValue = "") String order,
                                        @QueryValue(value = "search", defaultValue = "") String search) {
        String dbNameClean = unquoteBracket(dbName);
        String schemaClean = unquoteBracket(schema);
        String tableClean = unquoteBracket(table);
        String schemaEscaped = schemaClean != null ? schemaClean.replace("]", "]]") : "";
        String tableEscaped = tableClean != null ? tableClean.replace("]", "]]") : "";
        String sql = "SELECT * FROM [" + schemaEscaped + "].[" + tableEscaped + "]";
        Map<String, Object> model = buildSqlPageModel(id, dbNameClean != null ? dbNameClean : "", schemaClean != null ? schemaClean : "", sql, offset, limit, sort, order, search);
        model.put("tableDetailActionUrl", "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "") + "/" + (schemaClean != null ? schemaClean : "") + "/" + (tableClean != null ? tableClean : "") + "/detail");
        @SuppressWarnings("unchecked")
        List<BreadcrumbItem> breadcrumbs = (List<BreadcrumbItem>) model.get("breadcrumbs");
        if (breadcrumbs != null && !breadcrumbs.isEmpty()) {
            breadcrumbs.set(breadcrumbs.size() - 1, new BreadcrumbItem(tableClean != null ? tableClean : "", null));
        }
        ControllerModelHelper.refreshBreadcrumbPath(model);

        return model;
    }

    private Map<String, Object> buildSqlPageModel(Long id, String dbName, String schema, String sql,
                                                  Integer offset, Integer limit, String sort, String order, String search) {
        String dbNameClean = unquoteBracket(dbName);
        String schemaClean = unquoteBracket(schema);
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/sqlserver/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbNameClean != null ? dbNameClean : "", "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "")));
        breadcrumbs.add(new BreadcrumbItem(schemaClean != null ? schemaClean : "", "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "") + "/" + (schemaClean != null ? schemaClean : "")));
        breadcrumbs.add(new BreadcrumbItem("sql", null, false));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbNameClean != null ? dbNameClean : "");
        model.put("schema", schemaClean != null ? schemaClean : "");
        model.put("sql", sql != null ? sql : "");
        model.put("tableQueryActionUrl", "/sqlserver/" + id + "/query");
        String tableSegment = sqlServerMetadataService.parseTableFromSql(sql != null ? sql : "").map(SqlServerController::simpleTableName).orElse(null);
        model.put("tableDetailActionUrl", tableSegment != null && !tableSegment.isBlank()
                ? "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "") + "/" + (schemaClean != null ? schemaClean : "") + "/" + tableSegment + "/detail"
                : "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "") + "/" + (schemaClean != null ? schemaClean : "") + "/detail");
        model.put("includeAlertOob", false);

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? Math.min(limit, 1000) : 100;
        model.put("size", lim);

        if (sql == null || sql.isBlank()) {
            QueryResultModelHelper.putEmptyQueryPage(model, lim);
        } else {
            var result = sqlServerMetadataService.executeQuery(id, dbNameClean != null ? dbNameClean : "", sql, off, lim, sort, order, searchTerm)
                    .orElse(QueryResult.error(ErrorKeys.QUERY_EXECUTION_FAILED));
            QueryResultModelHelper.putQueryResult(model, result, sort, order);
        }

        return model;
    }

    @Post("/{id}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object executeQuery(@PathVariable Long id, String sql, String dbName, String schema,
                               @Nullable Integer offset, @Nullable Integer limit,
                               @Nullable String sort, @Nullable String order, @Nullable String search, String target) {
        String dbNameClean = unquoteBracket(dbName);
        String schemaClean = unquoteBracket(schema);
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("dbName", dbNameClean != null ? dbNameClean : "");
        model.put("schema", schemaClean != null ? schemaClean : "");
        model.put("includeAlertOob", true);
        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);

        if (sql == null || sql.isBlank()) {
            QueryResultModelHelper.putEmptyQueryHtmxAlert(model);
            model.put("queryActionUrl", "/sqlserver/" + id + "/query");
            model.put("tableQueryActionUrl", "/sqlserver/" + id + "/query");
            model.put("tableDetailActionUrl", "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "") + "/" + (schemaClean != null ? schemaClean : "") + "/detail");

            return "table".equals(target)
                    ? new ModelAndView<>("partials/table-view-result", model)
                    : new ModelAndView<>("partials/query-result", model);
        }
        model.put("queryActionUrl", "/sqlserver/" + id + "/query");
        model.put("tableQueryActionUrl", "/sqlserver/" + id + "/query");
        String tableSegment = sqlServerMetadataService.parseTableFromSql(sql).map(SqlServerController::simpleTableName).orElse(null);
        model.put("tableDetailActionUrl", tableSegment != null && !tableSegment.isBlank()
                ? "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "") + "/" + (schemaClean != null ? schemaClean : "") + "/" + tableSegment + "/detail"
                : "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "") + "/" + (schemaClean != null ? schemaClean : "") + "/detail");

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = sqlServerMetadataService.executeQuery(id, dbNameClean != null ? dbNameClean : "", sql, off, lim, sort, order, searchTerm)
                .orElse(QueryResult.error(ErrorKeys.QUERY_EXECUTION_FAILED));
        QueryResultModelHelper.putQueryResult(model, result, sort, order);
        model.put("sql", sql);

        return "table".equals(target)
                ? new ModelAndView<>("partials/table-view-result", model)
                : new ModelAndView<>("partials/query-result", model);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/{schema}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("sqlserver/detail")
    public Map<String, Object> rowDetail(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                        String sql, Integer rowNum, String sort, String order, @Nullable String search) {
        return rowDetail(id, dbName, schema, sql, rowNum, sort, order, search, null);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/{schema}/{table}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("sqlserver/detail")
    public Map<String, Object> rowDetailWithTable(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                                   @PathVariable String table, String sql, Integer rowNum, String sort, String order,
                                                   @Nullable String search) {
        return rowDetail(id, dbName, schema, sql, rowNum, sort, order, search, table);
    }

    private Map<String, Object> rowDetail(Long id, String dbName, String schema, String sql, Integer rowNum,
                                          String sort, String order, String search, @Nullable String tableParam) {
        String dbNameClean = unquoteBracket(dbName);
        String schemaClean = unquoteBracket(schema);
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String tableLabel = tableParam != null && !tableParam.isBlank() ? unquoteBracket(tableParam) : null;
        if (tableLabel == null || tableLabel.isBlank()) {
            tableLabel = sqlServerMetadataService.parseTableFromSql(sql != null ? sql : "").map(SqlServerController::simpleTableName).orElse(null);
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/sqlserver/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbNameClean != null ? dbNameClean : "", "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "")));
        String schemaUrl = (dbNameClean != null && schemaClean != null && !dbNameClean.isBlank() && !schemaClean.isBlank())
                ? "/sqlserver/" + id + "/" + dbNameClean + "/" + schemaClean
                : null;
        breadcrumbs.add(new BreadcrumbItem(schemaClean != null ? schemaClean : "", schemaUrl));
        if (tableLabel != null && !tableLabel.isBlank()) {
            breadcrumbs.add(new BreadcrumbItem(tableLabel, "/sqlserver/" + id + "/" + (dbNameClean != null ? dbNameClean : "") + "/" + (schemaClean != null ? schemaClean : "") + "/" + tableLabel));
        }
        breadcrumbs.add(new BreadcrumbItem("detail", null, false));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbNameClean != null ? dbNameClean : "");
        model.put("schema", schemaClean != null ? schemaClean : "");
        model.put("sql", sql != null ? sql : "");
        model.put("rowNum", rowNum != null ? rowNum : 0);
        model.put("sort", sort != null ? sort : "");
        model.put("order", order != null ? order : "");
        model.put("searchTerm", search != null && !search.isBlank() ? search.trim() : "");
        if (tableLabel != null && !tableLabel.isBlank()) {
            model.put("table", tableLabel);
        }

        if (sql != null && !sql.isBlank() && rowNum != null && rowNum >= 0) {
            Map<String, Object> detailResult = sqlServerMetadataService.getDetailRow(id, dbNameClean != null ? dbNameClean : "", schemaClean != null ? schemaClean : "", sql, Math.max(0, rowNum), sort, order);
            model.putAll(detailResult);
        } else {
            model.put("detailRows", List.<Map<String, String>>of());
            model.put("editable", false);
        }
        model.put("readOnly", readOnly);

        @SuppressWarnings("unchecked")
        List<Map<String, String>> detailRows = (List<Map<String, String>>) model.get("detailRows");
        if (detailRows != null && !detailRows.isEmpty()) {
            String label = conn.get().getName() + " / " + (dbNameClean != null ? dbNameClean : "") + " / " + (schemaClean != null ? schemaClean : "") + " / row " + (rowNum != null ? rowNum : 0);
            try {
                String dataJson = objectMapper.writeValueAsString(detailRows);
                Map<String, Object> payload = Map.of(
                        "source", "mssql",
                        "connectionId", id,
                        "connectionName", conn.get().getName(),
                        "label", label,
                        "data", dataJson,
                        "dataFormat", "keyValue"
                );
                model.put("dataDiffPayload", objectMapper.writeValueAsString(payload));
            } catch (JsonProcessingException e) {
                model.put("dataDiffPayload", (String) null);
            }
        } else {
            model.put("dataDiffPayload", (String) null);
        }

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
        assertNotReadOnly();
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
        Optional<String> parsedTable = sqlServerMetadataService.parseTableFromSql(sql);
        if (parsedTable.isEmpty()) {
            Map<String, Object> model = rowDetail(id, dbName, schema, sql, rowNum, sort, order, searchParam, tableParam);
            AppAlerts.i18n(model, ErrorKeys.TABLE_NOT_DETERMINED);
            return new ModelAndView<>("sqlserver/detail", model);
        }

        String tableRef = qualifiedTable != null && !qualifiedTable.isBlank() ? qualifiedTable : parsedTable.get();
        Optional<String> err = sqlServerMetadataService.executeUpdateByKey(id, unquoteBracket(dbName), unquoteBracket(schema), tableRef, uniqueKeyColumns, keyValues, columnValues);
        if (err.isPresent()) {
            Map<String, Object> model = rowDetail(id, dbName, schema, sql, rowNum, sort, order, searchParam, tableParam);
            AppAlerts.fromControllerMessage(model, err.get());
            return new ModelAndView<>("sqlserver/detail", model);
        }

        return new ModelAndView<>("sqlserver/detail", rowDetail(id, dbName, schema, sql, rowNum, sort, order, searchParam, tableParam));
    }

    private static String simpleTableName(String qualified) {
        if (qualified == null || qualified.isBlank()) {
            return "";
        }
        String trimmed = qualified.trim();
        int dotBracket = trimmed.lastIndexOf("].[");
        if (dotBracket >= 0 && dotBracket + 3 < trimmed.length()) {
            String part = trimmed.substring(dotBracket + 3);
            return unquoteBracket(part.endsWith("]") ? part.substring(0, part.length() - 1) : part);
        }
        return unquoteBracket(trimmed);
    }

    private static String unquoteBracket(String s) {
        if (s == null || s.isBlank()) {
            return s != null ? s : "";
        }
        String t = s.trim();
        if (t.length() >= 2 && t.charAt(0) == '[' && t.charAt(t.length() - 1) == ']') {
            return t.substring(1, t.length() - 1).replace("]]", "]");
        }
        return t;
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
