package com.panopticum.controller;

import com.panopticum.model.BreadcrumbItem;
import com.panopticum.model.DbConnection;
import com.panopticum.service.DbConnectionService;
import com.panopticum.service.PgMetadataService;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.ModelAndView;
import io.micronaut.views.View;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;

import java.util.ArrayList;
import java.util.HashMap;
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
    public Map<String, Object> databases(@PathVariable Long id) {
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
        List<String> dbs = pgMetadataService.listDatabases(id);
        model.put("items", dbs);
        model.put("itemType", "database");
        model.put("itemUrlPrefix", "/pg/" + id + "/");

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}")
    @View("pg/schemas")
    public Map<String, Object> schemas(@PathVariable Long id, @PathVariable String dbName) {
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
        List<String> schemas = pgMetadataService.listSchemas(id, dbName);
        model.put("items", schemas);
        model.put("itemType", "schema");
        model.put("itemUrlPrefix", "/pg/" + id + "/" + dbName + "/");

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{dbName}/{schema}")
    @View("pg/tables")
    public Map<String, Object> tables(@PathVariable Long id, @PathVariable String dbName, @PathVariable String schema) {
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
        List<PgMetadataService.TableInfo> tables = pgMetadataService.listTables(id, dbName, schema);
        model.put("tables", tables);

        return model;
    }

    @Post("/{id}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object executeQuery(@PathVariable Long id, String sql, String dbName, String schema) {
        Map<String, Object> model = new HashMap<>();
        model.put("connectionId", id);
        model.put("dbName", dbName);
        model.put("schema", schema);
        if (sql == null || sql.isBlank()) {
            model.put("error", "Empty query");

            return new ModelAndView<>("partials/query-result", model);
        }

        var result = pgMetadataService.executeQuery(id, sql).orElse(PgMetadataService.QueryResult.error("Execution failed"));
        model.put("error", result.hasError() ? result.getError() : null);
        model.put("columns", result.getColumns());
        model.put("rows", result.getRows());

        return new ModelAndView<>("partials/query-result", model);
    }

    private Map<String, Object> baseModel(Long id) {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        dbConnectionService.findById(id).ifPresent(conn -> model.put("connection", conn));

        return model;
    }
}
