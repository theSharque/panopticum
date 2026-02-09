package com.panopticum.core.controller;

import com.panopticum.cassandra.service.CassandraMetadataService;
import com.panopticum.clickhouse.service.ClickHouseMetadataService;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.i18n.LocaleFilter;
import com.panopticum.i18n.Messages;
import com.panopticum.mongo.service.MongoMetadataService;
import com.panopticum.mysql.service.MySqlMetadataService;
import com.panopticum.postgres.service.PgMetadataService;
import com.panopticum.redis.service.RedisMetadataService;
import com.panopticum.config.HxRedirectFilter;
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
    private final PgMetadataService pgMetadataService;
    private final MongoMetadataService mongoMetadataService;
    private final RedisMetadataService redisMetadataService;
    private final ClickHouseMetadataService clickHouseMetadataService;
    private final MySqlMetadataService mySqlMetadataService;
    private final CassandraMetadataService cassandraMetadataService;

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
        DbConnection conn = DbConnection.builder()
                .name(name != null ? name : "")
                .type("postgresql")
                .host(host != null ? host : "localhost")
                .port(port != null ? port : 5432)
                .dbName(database != null ? database : "")
                .username(username != null ? username : "")
                .password(password != null ? password : "")
                .build();
        DbConnection saved = dbConnectionService.save(conn);

        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            if (saved.getId() != null) {
                request.setAttribute(HxRedirectFilter.HX_REDIRECT_ATTR, "/pg/" + saved.getId());
            }
            return new ModelAndView<>("partials/sidebar", model);
        }

        return new ModelAndView<>("settings/index", model);
    }

    @Post("/test-postgres")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testPostgres(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        Map<String, Object> model = new HashMap<>();
        try {
            int p = port != null ? port : 5432;
            var error = pgMetadataService.testConnection(
                    host != null ? host : "",
                    p,
                    database != null ? database : "",
                    username != null ? username : "",
                    password != null ? password : "");
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

    @Post("/add-mongo")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addMongo(HttpRequest<?> request,
                          String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = DbConnection.builder()
                .name(name != null ? name : "")
                .type("mongodb")
                .host(host != null ? host : "localhost")
                .port(port != null ? port : 27017)
                .dbName(database != null ? database : "")
                .username(username != null ? username : "")
                .password(password != null ? password : "")
                .build();
        DbConnection saved = dbConnectionService.save(conn);

        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            if (saved.getId() != null) {
                request.setAttribute(HxRedirectFilter.HX_REDIRECT_ATTR, "/mongo/" + saved.getId());
            }
            return new ModelAndView<>("partials/sidebar", model);
        }

        return new ModelAndView<>("settings/index", model);
    }

    @Post("/test-mongo")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testMongo(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        Map<String, Object> model = new HashMap<>();
        int p = port != null ? port : 27017;
        var error = mongoMetadataService.testConnection(
                host != null ? host : "",
                p,
                database != null ? database : "",
                username != null ? username : "",
                password != null ? password : "");
        model.put("success", error.isEmpty());
        String messageKey = error.orElse("connectionTest.success");
        model.put("message", messageKey);
        putDisplayText(model, request, messageKey);

        return new ModelAndView<>("partials/connection-test-result", model);
    }

    @Post("/add-redis")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addRedis(HttpRequest<?> request,
                          String name, String host, Integer port, String database, String password) {
        DbConnection conn = DbConnection.builder()
                .name(name != null ? name : "")
                .type("redis")
                .host(host != null ? host : "localhost")
                .port(port != null ? port : 6379)
                .dbName(database != null && !database.isBlank() ? database : "0")
                .username("")
                .password(password != null ? password : "")
                .build();
        DbConnection saved = dbConnectionService.save(conn);

        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            if (saved.getId() != null) {
                request.setAttribute(HxRedirectFilter.HX_REDIRECT_ATTR, "/redis/" + saved.getId());
            }
            return new ModelAndView<>("partials/sidebar", model);
        }

        return new ModelAndView<>("settings/index", model);
    }

    @Post("/test-redis")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testRedis(HttpRequest<?> request,
            String host, Integer port, String database, String password) {
        Map<String, Object> model = new HashMap<>();
        int p = port != null ? port : 6379;
        int dbIndex = 0;
        if (database != null && !database.isBlank()) {
            try {
                dbIndex = Integer.parseInt(database.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        var error = redisMetadataService.testConnection(
                host != null ? host : "",
                p,
                password != null ? password : "",
                dbIndex);
        model.put("success", error.isEmpty());
        String messageKey = error.orElse("connectionTest.success");
        model.put("message", messageKey);
        putDisplayText(model, request, messageKey);

        return new ModelAndView<>("partials/connection-test-result", model);
    }

    @Post("/add-clickhouse")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addClickhouse(HttpRequest<?> request,
                               String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = DbConnection.builder()
                .name(name != null ? name : "")
                .type("clickhouse")
                .host(host != null ? host : "localhost")
                .port(port != null ? port : 8123)
                .dbName(database != null && !database.isBlank() ? database : "default")
                .username(username != null ? username : "")
                .password(password != null ? password : "")
                .build();
        DbConnection saved = dbConnectionService.save(conn);

        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            if (saved.getId() != null) {
                request.setAttribute(HxRedirectFilter.HX_REDIRECT_ATTR, "/ch/" + saved.getId());
            }
            return new ModelAndView<>("partials/sidebar", model);
        }

        return new ModelAndView<>("settings/index", model);
    }

    @Post("/test-clickhouse")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testClickhouse(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        Map<String, Object> model = new HashMap<>();
        int p = port != null ? port : 8123;
        var error = clickHouseMetadataService.testConnection(
                host != null ? host : "",
                p,
                database != null ? database : "default",
                username != null ? username : "",
                password != null ? password : "");
        model.put("success", error.isEmpty());
        String messageKey = error.orElse("connectionTest.success");
        model.put("message", messageKey);
        putDisplayText(model, request, messageKey);

        return new ModelAndView<>("partials/connection-test-result", model);
    }

    @Post("/add-mysql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addMysql(HttpRequest<?> request,
                          String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = DbConnection.builder()
                .name(name != null ? name : "")
                .type("mysql")
                .host(host != null ? host : "localhost")
                .port(port != null ? port : 3306)
                .dbName(database != null ? database : "")
                .username(username != null ? username : "")
                .password(password != null ? password : "")
                .build();
        DbConnection saved = dbConnectionService.save(conn);

        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            if (saved.getId() != null) {
                request.setAttribute(HxRedirectFilter.HX_REDIRECT_ATTR, "/mysql/" + saved.getId());
            }
            return new ModelAndView<>("partials/sidebar", model);
        }

        return new ModelAndView<>("settings/index", model);
    }

    @Post("/test-mysql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testMysql(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        Map<String, Object> model = new HashMap<>();
        try {
            int p = port != null ? port : 3306;
            var error = mySqlMetadataService.testConnection(
                    host != null ? host : "",
                    p,
                    database != null ? database : "",
                    username != null ? username : "",
                    password != null ? password : "");
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

    @Post("/add-cassandra")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addCassandra(HttpRequest<?> request,
                              String name, String host, Integer port, String database, String username, String password) {
        DbConnection conn = DbConnection.builder()
                .name(name != null ? name : "")
                .type("cassandra")
                .host(host != null ? host : "localhost")
                .port(port != null ? port : 9042)
                .dbName(database != null ? database : "")
                .username(username != null ? username : "")
                .password(password != null ? password : "")
                .build();
        DbConnection saved = dbConnectionService.save(conn);

        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            if (saved.getId() != null) {
                request.setAttribute(HxRedirectFilter.HX_REDIRECT_ATTR, "/cassandra/" + saved.getId());
            }
            return new ModelAndView<>("partials/sidebar", model);
        }

        return new ModelAndView<>("settings/index", model);
    }

    @Post("/test-cassandra")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testCassandra(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password) {
        Map<String, Object> model = new HashMap<>();
        try {
            int p = port != null ? port : 9042;
            var error = cassandraMetadataService.testConnection(
                    host != null ? host : "",
                    p,
                    database != null ? database : "",
                    username != null ? username : "",
                    password != null ? password : "");
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

    private void putDisplayText(Map<String, Object> model, HttpRequest<?> request, String messageKey) {
        String locale = (String) request.getAttribute(LocaleFilter.LOCALE_ATTR).orElse("en");
        String text = Messages.forLocale(locale).getOrDefault(messageKey, messageKey);
        model.put("displayText", text);
    }
}
