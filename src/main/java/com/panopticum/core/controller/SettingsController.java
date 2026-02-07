package com.panopticum.core.controller;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.mongo.service.MongoMetadataService;
import com.panopticum.postgres.service.PgMetadataService;
import com.panopticum.redis.service.RedisMetadataService;
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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Controller("/settings")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
public class SettingsController {

    private static final String HX_REQUEST = "HX-Request";

    private final DbConnectionService dbConnectionService;
    private final PgMetadataService pgMetadataService;
    private final MongoMetadataService mongoMetadataService;
    private final RedisMetadataService redisMetadataService;

    public SettingsController(DbConnectionService dbConnectionService, PgMetadataService pgMetadataService,
                              MongoMetadataService mongoMetadataService, RedisMetadataService redisMetadataService) {
        this.dbConnectionService = dbConnectionService;
        this.pgMetadataService = pgMetadataService;
        this.mongoMetadataService = mongoMetadataService;
        this.redisMetadataService = redisMetadataService;
    }

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
        dbConnectionService.save(conn);

        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            return new ModelAndView<>("partials/sidebar", model);
        }

        return new ModelAndView<>("settings/index", model);
    }

    @Post("/test-postgres")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testPostgres(
            String host, Integer port, String database, String username, String password) {
        Map<String, Object> model = new HashMap<>();
        int p = port != null ? port : 5432;
        var error = pgMetadataService.testConnection(
                host != null ? host : "",
                p,
                database != null ? database : "",
                username != null ? username : "",
                password != null ? password : "");
        model.put("success", error.isEmpty());
        model.put("message", error.orElse("connectionTest.success"));

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
        dbConnectionService.save(conn);

        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            return new ModelAndView<>("partials/sidebar", model);
        }

        return new ModelAndView<>("settings/index", model);
    }

    @Post("/test-mongo")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testMongo(
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
        model.put("message", error.orElse("connectionTest.success"));

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
        dbConnectionService.save(conn);

        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));

        if (hxRequest) {
            return new ModelAndView<>("partials/sidebar", model);
        }

        return new ModelAndView<>("settings/index", model);
    }

    @Post("/test-redis")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testRedis(
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
        model.put("message", error.orElse("connectionTest.success"));

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
}
