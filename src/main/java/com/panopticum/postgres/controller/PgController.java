package com.panopticum.postgres.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.postgres.model.PgDatabaseInfo;
import com.panopticum.postgres.model.PgSchemaInfo;
import com.panopticum.postgres.model.TableInfo;
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
public class PgController {

    private final DbConnectionService dbConnectionService;
    private final PgMetadataService pgMetadataService;

    public PgController(DbConnectionService dbConnectionService, PgMetadataService pgMetadataService) {
        this.dbConnectionService = dbConnectionService;
        this.pgMetadataService = pgMetadataService;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("pg/databases")
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
        model.put("schema", null);
        model.put("itemType", "database");
        model.put("itemUrlPrefix", "/pg/" + id + "/");

        Page<PgDatabaseInfo> paged = pgMetadataService.listDatabasesPaged(id, page, size, sort, order);
        model.put("items", paged.getItems());
        model.put("page", paged.getPage());
        model.put("size", paged.getSize());
        model.put("sort", paged.getSort());
        model.put("order", paged.getOrder());
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
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/pg/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("schema", null);
        model.put("itemType", "schema");
        model.put("itemUrlPrefix", "/pg/" + id + "/" + dbName + "/");

        Page<PgSchemaInfo> paged = pgMetadataService.listSchemasPaged(id, dbName, page, size, sort, order);
        model.put("items", paged.getItems());
        model.put("page", paged.getPage());
        model.put("size", paged.getSize());
        model.put("sort", paged.getSort());
        model.put("order", paged.getOrder());
        model.put("fromRow", paged.getFromRow());
        model.put("toRow", paged.getToRow());
        model.put("hasPrev", paged.isHasPrev());
        model.put("hasMore", paged.isHasMore());
        model.put("prevOffset", paged.getPrevOffset());
        model.put("nextOffset", paged.getNextOffset());

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
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/pg/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/pg/" + id + "/" + dbName));
        breadcrumbs.add(new BreadcrumbItem(schema, null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("schema", schema);

        Page<TableInfo> paged = pgMetadataService.listTablesPaged(id, dbName, schema, page, size, sort, order);
        model.put("tables", paged.getItems());
        model.put("page", paged.getPage());
        model.put("size", paged.getSize());
        model.put("sort", paged.getSort());
        model.put("order", paged.getOrder());
        model.put("fromRow", paged.getFromRow());
        model.put("toRow", paged.getToRow());
        model.put("hasPrev", paged.isHasPrev());
        model.put("hasMore", paged.isHasMore());
        model.put("prevOffset", paged.getPrevOffset());
        model.put("nextOffset", paged.getNextOffset());

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
                                         @QueryValue(value = "order", defaultValue = "") String order) {
        return buildSqlPageModel(id, dbName, schema, sql, offset, limit, sort, order);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/{dbName}/{schema}/sql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("pg/sql")
    public Map<String, Object> sqlPagePost(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                          String sql, @Nullable Integer offset, @Nullable Integer limit,
                                          @Nullable String sort, @Nullable String order) {
        return buildSqlPageModel(id, dbName, schema, sql, offset, limit, sort, order);
    }

    private Map<String, Object> buildSqlPageModel(Long id, String dbName, String schema, String sql,
                                                  Integer offset, Integer limit, String sort, String order) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/pg/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName, "/pg/" + id + "/" + dbName));
        breadcrumbs.add(new BreadcrumbItem(schema, "/pg/" + id + "/" + dbName + "/" + schema));
        breadcrumbs.add(new BreadcrumbItem("sql", null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("schema", schema);
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
            var result = pgMetadataService.executeQuery(id, dbName, sql, off, lim, sort, order)
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
    public Object executeQuery(@PathVariable Long id, String sql, String dbName, String schema,
                              @Nullable Integer offset, @Nullable Integer limit,
                              @Nullable String sort, @Nullable String order, String target) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("schema", schema);

        if (sql == null || sql.isBlank()) {
            model.put("error", "Empty query");

            return "table".equals(target)
                    ? new ModelAndView<>("partials/table-view-result", model)
                    : new ModelAndView<>("partials/query-result", model);
        }

        int off = offset != null ? Math.max(0, offset) : 0;
        int lim = limit != null && limit > 0 ? limit : 100;
        var result = pgMetadataService.executeQuery(id, dbName, sql, off, lim, sort, order)
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
    @Post("/{id}/{dbName}/{schema}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("pg/detail")
    public Map<String, Object> rowDetail(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema,
                                         String sql, Integer rowNum, String sort, String order) {
        Map<String, Object> model = baseModel(id);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/pg/" + id));
        breadcrumbs.add(new BreadcrumbItem(dbName != null ? dbName : "", "/pg/" + id + "/" + (dbName != null ? dbName : "")));
        String schemaUrl = (dbName != null && schema != null && !dbName.isBlank() && !schema.isBlank())
                ? "/pg/" + id + "/" + dbName + "/" + schema
                : null;
        breadcrumbs.add(new BreadcrumbItem(schema != null ? schema : "", schemaUrl));
        breadcrumbs.add(new BreadcrumbItem("detail", null));
        model.put("breadcrumbs", breadcrumbs);
        model.put("connectionId", id);
        model.put("dbName", dbName != null ? dbName : "");
        model.put("schema", schema != null ? schema : "");
        model.put("sql", sql != null ? sql : "");
        model.put("rowNum", rowNum != null ? rowNum : 0);
        model.put("sort", sort != null ? sort : "");
        model.put("order", order != null ? order : "");

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

        Optional<String> qualifiedTable = pgMetadataService.parseTableFromSql(sql);
        if (qualifiedTable.isEmpty()) {
            Map<String, Object> model = rowDetail(id, dbName, schema, sql, rowNum, sort, order);
            model.put("error", "Could not determine table from SQL.");

            return new ModelAndView<>("pg/detail", model);
        }

        Optional<String> err = pgMetadataService.executeUpdateByCtid(id, dbName, qualifiedTable.get(), ctid, columnValues);
        if (err.isPresent()) {
            Map<String, Object> model = rowDetail(id, dbName, schema, sql, rowNum, sort, order);
            model.put("error", err.get());

            return new ModelAndView<>("pg/detail", model);
        }

        return new ModelAndView<>("pg/detail", rowDetail(id, dbName, schema, sql, rowNum, sort, order));
    }

    private Map<String, Object> baseModel(Long id) {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        dbConnectionService.findById(id).ifPresent(conn -> model.put("connection", conn));

        return model;
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
