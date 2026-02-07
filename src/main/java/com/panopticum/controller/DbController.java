package com.panopticum.controller;

import com.panopticum.service.DbConnectionService;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.View;

import java.util.HashMap;
import java.util.Map;

@Controller("/db")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class DbController {

    private final DbConnectionService dbConnectionService;

    public DbController(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}")
    @View("db/stub")
    public Map<String, Object> show(@PathVariable Long id) {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        dbConnectionService.findById(id).ifPresent(conn -> model.put("connection", conn));
        return model;
    }
}
