package com.panopticum.core.controller;

import com.panopticum.config.HxRedirectFilter;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.ConnectionTestService;
import com.panopticum.core.service.DbConnectionFactory;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.i18n.LocaleFilter;
import com.panopticum.i18n.Messages;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.ModelAndView;
import io.micronaut.views.View;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Controller("/settings")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class SettingsController {

    private static final String HX_REQUEST = "HX-Request";

    private final DbConnectionService dbConnectionService;
    private final DbConnectionFactory dbConnectionFactory;
    private final ConnectionTestService connectionTestService;

    @Produces(MediaType.TEXT_HTML)
    @Get
    @View("settings/index")
    public Map<String, Object> index() {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return model;
    }

    @Post("/add-postgres")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addPostgres(HttpRequest<?> request,
                             String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = dbConnectionFactory.build("postgresql", name, host, port, database, username, password);
        DbConnection saved = dbConnectionService.save(conn);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, saved.getId(), "/pg/" + saved.getId());
    }

    @Post("/test-postgres")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testPostgres(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        return testConnectionResult(request, "postgresql", host, port, database, username, password);
    }

    @Post("/add-mongo")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addMongo(HttpRequest<?> request,
                          String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = dbConnectionFactory.build("mongodb", name, host, port, database, username, password);
        DbConnection saved = dbConnectionService.save(conn);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, saved.getId(), "/mongo/" + saved.getId());
    }

    @Post("/test-mongo")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testMongo(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        return testConnectionResult(request, "mongodb", host, port, database, username, password);
    }

    @Post("/add-redis")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addRedis(HttpRequest<?> request,
                          String name, String host, Integer port, String database, String password) {
        DbConnection conn = dbConnectionFactory.build("redis", name, host, port, database, null, password);
        DbConnection saved = dbConnectionService.save(conn);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, saved.getId(), "/redis/" + saved.getId());
    }

    @Post("/test-redis")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testRedis(HttpRequest<?> request,
            String host, Integer port, String database, String password) {
        return testConnectionResult(request, "redis", host, port, database, null, password);
    }

    @Post("/add-clickhouse")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addClickhouse(HttpRequest<?> request,
                               String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = dbConnectionFactory.build("clickhouse", name, host, port, database, username, password);
        DbConnection saved = dbConnectionService.save(conn);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, saved.getId(), "/ch/" + saved.getId());
    }

    @Post("/test-clickhouse")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testClickhouse(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        return testConnectionResult(request, "clickhouse", host, port, database, username, password);
    }

    @Post("/add-mysql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addMysql(HttpRequest<?> request,
                          String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = dbConnectionFactory.build("mysql", name, host, port, database, username, password);
        DbConnection saved = dbConnectionService.save(conn);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, saved.getId(), "/mysql/" + saved.getId());
    }

    @Post("/test-mysql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testMysql(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        return testConnectionResult(request, "mysql", host, port, database, username, password);
    }

    @Post("/add-mssql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addMssql(HttpRequest<?> request,
                           String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = dbConnectionFactory.build("sqlserver", name, host, port, database, username, password);
        DbConnection saved = dbConnectionService.save(conn);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, saved.getId(), "/mssql/" + saved.getId());
    }

    @Post("/test-mssql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testMssql(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        return testConnectionResult(request, "sqlserver", host, port, database, username, password);
    }

    @Post("/add-oracle")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addOracle(HttpRequest<?> request,
                            String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = dbConnectionFactory.build("oracle", name, host, port, database, username, password);
        DbConnection saved = dbConnectionService.save(conn);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, saved.getId(), "/oracle/" + saved.getId());
    }

    @Post("/test-oracle")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testOracle(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        return testConnectionResult(request, "oracle", host, port, database, username, password);
    }

    @Post("/add-cassandra")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addCassandra(HttpRequest<?> request,
                              String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = dbConnectionFactory.build("cassandra", name, host, port, database, username, password);
        DbConnection saved = dbConnectionService.save(conn);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, saved.getId(), "/cassandra/" + saved.getId());
    }

    @Post("/test-cassandra")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testCassandra(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        return testConnectionResult(request, "cassandra", host, port, database, username, password);
    }

    @Delete("/delete-connection/{id}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object deleteConnection(HttpRequest<?> request, @PathVariable Long id) {
        dbConnectionService.deleteById(id);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            return new ModelAndView<>("partials/sidebar", model);
        }

        return HttpResponse.redirect(URI.create("/settings"));
    }

    private Object responseAfterAdd(HttpRequest<?> request, Map<String, Object> model,
                                    Long savedId, String redirectPath) {
        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));
        if (hxRequest) {
            if (savedId != null) {
                request.setAttribute(HxRedirectFilter.HX_REDIRECT_ATTR, redirectPath);
            }
            return new ModelAndView<>("partials/sidebar", model);
        }
        return new ModelAndView<>("settings/index", model);
    }

    private ModelAndView<Map<String, Object>> testConnectionResult(HttpRequest<?> request, String type,
                                                                   String host, Integer port, String database,
                                                                   String username, String password) {
        Map<String, Object> model = new HashMap<>();
        try {
            var error = connectionTestService.test(type, host, port, database, username, password);
            model.put("success", error.isEmpty());
            String messageKey = error.orElse("connectionTest.success");
            model.put("message", messageKey);
            putDisplayText(model, request, messageKey);
        } catch (Exception e) {
            model.put("success", false);
            String messageKey = e.getMessage() != null ? e.getMessage() : "error.queryExecutionFailed";
            model.put("message", messageKey);
            putDisplayText(model, request, messageKey);
        }
        return new ModelAndView<>("partials/connection-test-result", model);
    }

    private void putDisplayText(Map<String, Object> model, HttpRequest<?> request, String messageKey) {
        String locale = (String) request.getAttribute(LocaleFilter.LOCALE_ATTR).orElse("en");
        String text = Messages.forLocale(locale).getOrDefault(messageKey, messageKey);
        model.put("displayText", text);
    }
}
