package com.panopticum.oracle.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.oracle.service.OracleMetadataService;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
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

@Controller("/oracle")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class OracleController {

    private final DbConnectionService dbConnectionService;
    private final OracleMetadataService oracleMetadataService;
    @Value("${panopticum.read-only:false}")
    private boolean readOnly;

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("oracle/schemas")
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
        model.put("schema", null);
        model.put("itemType", "schema");
        model.put("itemUrlPrefix", "/oracle/" + id + "/");

        Page<SchemaInfo> paged = oracleMetadataService.listSchemasPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "owner", "orderOwner", "tables", "orderTables"));

        return model;
    }

    @Get("/{id}/detail")
    public HttpResponse<?> detailRedirect(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/oracle/" + id));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{schema}")
    @View("oracle/tables")
    public Map<String, Object> tables(@PathVariable Long id, @PathVariable String schema,
                                     @QueryValue(value = "page", defaultValue = "1") int page,
                                     @QueryValue(value = "size", defaultValue = "50") int size,
                                     @QueryValue(value = "sort", defaultValue = "name") String sort,
                                     @QueryValue(value = "order", defaultValue = "asc") String order) {
        String schemaClean = unquotePgIdentifier(schema);
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/oracle/" + id));
        breadcrumbs.add(new BreadcrumbItem(schemaClean != null ? schemaClean : "", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("schema", schemaClean != null ? schemaClean : "");

        Page<TableInfo> paged = oracleMetadataService.listTablesPaged(id, schemaClean != null ? schemaClean : "", page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "tables");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "type", "orderType", "rows", "orderRows", "size", "orderSize"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{schema}/sql")
    @View("oracle/sql")
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
    @View("oracle/sql")
    public Map<String, Object> sqlPagePost(@PathVariable Long id, @PathVariable String schema,
                                          String sql, @Nullable Integer offset, @Nullable Integer limit,
                                          @Nullable String sort, @Nullable String order, @Nullable String search) {
        return buildSqlPageModel(id, schema, sql, offset, limit, sort, order, search);
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{schema}/{table}")
    @View("oracle/table")
    public Map<String, Object> tablePage(@PathVariable Long id, @PathVariable String schema, @PathVariable String table,
                                         @QueryValue(value = "offset", defaultValue = "0") Integer offset,
                                         @QueryValue(value = "limit", defaultValue = "100") Integer limit,
                                         @QueryValue(value = "sort", defaultValue = "") String sort,
                                         @QueryValue(value = "order", defaultValue = "") String order,
                                         @QueryValue(value = "search", defaultValue = "") String search) {
        String schemaClean = unquotePgIdentifier(schema);
        String tableClean = unquotePgIdentifier(table);
        String schemaEscaped = schemaClean != null ? schemaClean.replace("\"", "\"\"") : "";
        String tableEscaped = tableClean != null ? tableClean.replace("\"", "\"\"") : "";
        String sql = "SELECT * FROM \"" + schemaEscaped + "\".\"" + tableEscaped + "\"";
        Map<String, Object> model = buildSqlPageModel(id, schemaClean != null ? schemaClean : "", sql, offset, limit, sort, order, search);
        model.put("tableDetailActionUrl", "/oracle/" + id + "/" + (schemaClean != null ? schemaClean : "") + "/" + (tableClean != null ? tableClean : "") + "/detail");
        @SuppressWarnings("unchecked")
        List<BreadcrumbItem> breadcrumbs = (List<BreadcrumbItem>) model.get("breadcrumbs");
        if (breadcrumbs != null && !breadcrumbs.isEmpty()) {
            breadcrumbs.set(breadcrumbs.size() - 1, new BreadcrumbItem(tableClean != null ? tableClean : "", null));
        }
        return model;
    }

    private Map<String, Object> buildSqlPageModel(Long id, String schema, String sql,
                                                  Integer offset, Integer limit, String sort, String order, String search) {
        String schemaClean = unquotePgIdentifier(schema);
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/oracle/" + id));
        breadcrumbs.add(new BreadcrumbItem(schemaClean != null ? schemaClean : "", "/oracle/" + id + "/" + (schemaClean != null ? schemaClean : "")));
        breadcrumbs.add(new BreadcrumbItem("sql", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("schema", schemaClean != null ? schemaClean : "");
        model.put("sql", sql != null ? sql : "");
        model.put("tableQueryActionUrl", "/oracle/" + id + "/query");
        String tableSegment = oracleMetadataService.parseTableFromSql(sql != null ? sql : "").map(OracleController::simpleTableName).orElse(null);
        model.put("tableDetailActionUrl", tableSegment != null && !tableSegment.isBlank()
                ? "/oracle/" + id + "/" + (schemaClean != null ? schemaClean : "") + "/" + tableSegment + "/detail"
                : "/oracle/" + id + "/" + (schemaClean != null ? schemaClean : "") + "/detail");

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
            var result = oracleMetadataService.executeQuery(id, schemaClean != null ? schemaClean : "", sql, off, lim, sort, order, searchTerm)
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
    public Object executeQuery(@PathVariable Long id, String sql, @Nullable String schema, @Nullable String dbName,
                               @Nullable Integer offset, @Nullable Integer limit,
                               @Nullable String sort, @Nullable String order, @Nullable String search, String target) {
        String schemaParam = (schema != null && !schema.isBlank()) ? schema : dbName;
        String schemaClean = unquotePgIdentifier(schemaParam);
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("schema", schemaClean != null ? schemaClean : "");
        model.put("dbName", schemaClean);
        String searchTerm = search != null && !search.isBlank() ? search.trim() : "";
        model.put("searchTerm", searchTerm);

        if (sql == null || sql.isBlank()) {
            model.put("error", "Empty query");
            model.put("queryActionUrl", "/oracle/" + id + "/query");
            model.put("tableQueryActionUrl", "/oracle/" + id + "/query");
            model.put("tableDetailActionUrl", "/oracle/" + id + "/" + (schemaClean != null ? schemaClean : "") + "/detail");

            return "table".equals(target)
                    ? new ModelAndView<>("partials/table-view-result", model)
                    : new ModelAndView<>("partials/query-result", model);
        }
        model.put("queryActionUrl", "/oracle/" + id + "/query");
        model.put("tableQueryActionUrl", "/oracle/" + id + "/query");
        String tableSegment = oracleMetadataService.parseTableFromSql(sql).map(OracleController::simpleTableName).orElse(null);
        model.put("tableDetailActionUrl", tableSegment != null && !tableSegment.isBlank()
                ? "/oracle/" + id + "/" + (schemaClean != null ? schemaClean : "") + "/" + tableSegment + "/detail"
                : "/oracle/" + id + "/" + (schemaClean != null ? schemaClean : "") + "/detail");

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = oracleMetadataService.executeQuery(id, schemaClean != null ? schemaClean : "", sql, off, lim, sort, order, searchTerm)
                .orElse(QueryResult.error("Execution failed"));
        putQueryResultIntoModel(model, result, sql, sort, order);
        model.put("sql", sql);

        return "table".equals(target)
                ? new ModelAndView<>("partials/table-view-result", model)
                : new ModelAndView<>("partials/query-result", model);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{schema}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("oracle/detail")
    public Map<String, Object> rowDetail(@PathVariable Long id, @PathVariable String schema,
                                         String sql, Integer rowNum, String sort, String order, @Nullable String search) {
        return rowDetail(id, schema, sql, rowNum, sort, order, search, null);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{schema}/{table}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("oracle/detail")
    public Map<String, Object> rowDetailWithTable(@PathVariable Long id, @PathVariable String schema, @PathVariable String table,
                                                   String sql, Integer rowNum, String sort, String order, @Nullable String search) {
        return rowDetail(id, schema, sql, rowNum, sort, order, search, table);
    }

    private Map<String, Object> rowDetail(Long id, String schema, String sql, Integer rowNum,
                                         String sort, String order, String search, @Nullable String tableParam) {
        String schemaClean = unquotePgIdentifier(schema);
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String tableLabel = tableParam != null && !tableParam.isBlank() ? unquotePgIdentifier(tableParam) : null;
        if (tableLabel == null || tableLabel.isBlank()) {
            tableLabel = oracleMetadataService.parseTableFromSql(sql != null ? sql : "").map(OracleController::simpleTableName).orElse(null);
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/oracle/" + id));
        breadcrumbs.add(new BreadcrumbItem(schemaClean != null ? schemaClean : "", "/oracle/" + id + "/" + (schemaClean != null ? schemaClean : "")));
        if (tableLabel != null && !tableLabel.isBlank()) {
            breadcrumbs.add(new BreadcrumbItem(tableLabel, "/oracle/" + id + "/" + (schemaClean != null ? schemaClean : "") + "/" + tableLabel));
        }
        breadcrumbs.add(new BreadcrumbItem("detail", null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
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
            Map<String, Object> detailResult = oracleMetadataService.getDetailRowWithRowid(id, schemaClean != null ? schemaClean : "", sql,
                    Math.max(0, rowNum), sort, order);
            model.putAll(detailResult);
        } else {
            model.put("detailRows", List.<Map<String, String>>of());
            model.put("rowRowid", "");
        }
        model.put("readOnly", readOnly);

        return model;
    }

    private void assertNotReadOnly() {
        if (readOnly) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "read.only.enabled");
        }
    }

    @Post("/{id}/{schema}/detail/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveRow(@PathVariable Long id, @PathVariable String schema,
                         @Body Map<String, String> form) {
        return saveRow(id, schema, form, null);
    }

    @Post("/{id}/{schema}/{table}/detail/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveRowWithTable(@PathVariable Long id, @PathVariable String schema, @PathVariable String table,
                                   @Body Map<String, String> form) {
        return saveRow(id, schema, form, table);
    }

    private Object saveRow(Long id, String schema, Map<String, String> form, @Nullable String tableParam) {
        assertNotReadOnly();
        String sql = form != null ? form.get("sql") : null;
        Integer rowNum = form != null && form.containsKey("rowNum") ? parseInteger(form.get("rowNum")) : null;
        String sort = form != null ? form.get("sort") : null;
        String order = form != null ? form.get("order") : null;
        String rowid = form != null ? form.get("rowid") : null;
        Map<String, String> columnValues = new LinkedHashMap<>();
        if (form != null) {
            for (Map.Entry<String, String> e : form.entrySet()) {
                if (e.getKey() != null && e.getKey().startsWith("field_")) {
                    columnValues.put(e.getKey().substring(6), e.getValue() != null ? e.getValue() : "");
                }
            }
        }

        String searchParam = form != null ? form.get("search") : null;
        Optional<String> qualifiedTable = oracleMetadataService.parseTableFromSql(sql);
        if (qualifiedTable.isEmpty()) {
            Map<String, Object> model = rowDetail(id, schema, sql, rowNum, sort, order, searchParam, tableParam);
            model.put("error", "Could not determine table from SQL.");
            return new ModelAndView<>("oracle/detail", model);
        }

        Optional<String> err = oracleMetadataService.executeUpdateByRowid(id, unquotePgIdentifier(schema), qualifiedTable.get(), rowid, columnValues);
        if (err.isPresent()) {
            Map<String, Object> model = rowDetail(id, schema, sql, rowNum, sort, order, searchParam, tableParam);
            model.put("error", err.get());
            return new ModelAndView<>("oracle/detail", model);
        }

        return new ModelAndView<>("oracle/detail", rowDetail(id, schema, sql, rowNum, sort, order, searchParam, tableParam));
    }

    private static String simpleTableName(String qualified) {
        if (qualified == null || qualified.isBlank()) {
            return "";
        }
        String trimmed = qualified.trim();
        int dot = trimmed.lastIndexOf('.');
        if (dot >= 0 && dot + 1 < trimmed.length()) {
            return unquotePgIdentifier(trimmed.substring(dot + 1));
        }
        return unquotePgIdentifier(trimmed);
    }

    private static String unquotePgIdentifier(String s) {
        if (s == null || s.isBlank()) {
            return s != null ? s : "";
        }
        String t = s.trim();
        if (t.length() >= 2 && t.charAt(0) == '"' && t.charAt(t.length() - 1) == '"') {
            return t.substring(1, t.length() - 1).replace("\"\"", "\"");
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
