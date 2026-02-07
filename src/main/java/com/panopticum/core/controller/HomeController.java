package com.panopticum.core.controller;

import com.panopticum.core.service.DbConnectionService;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.View;

import java.util.HashMap;
import java.util.Map;

@Controller("/")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class HomeController {

    private final DbConnectionService dbConnectionService;

    public HomeController(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get
    @View("home")
    public Map<String, Object> index() {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return model;
    }
}
