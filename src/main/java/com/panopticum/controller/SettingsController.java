package com.panopticum.controller;

import com.panopticum.model.DbConnection;
import com.panopticum.service.DbConnectionService;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.View;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;

import java.util.HashMap;
import java.util.Map;

@Controller("/settings")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
public class SettingsController {

    private static final String HX_REQUEST = "HX-Request";

    private final DbConnectionService dbConnectionService;

    public SettingsController(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
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
            return new io.micronaut.views.ModelAndView<>("partials/sidebar", model);
        }

        return new io.micronaut.views.ModelAndView<>("settings/index", model);
    }
}
