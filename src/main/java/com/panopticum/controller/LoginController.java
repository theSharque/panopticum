package com.panopticum.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.View;

import java.security.Principal;
import java.util.Collections;
import java.util.Map;

@Controller("/login")
@Secured(SecurityRule.IS_ANONYMOUS)
public class LoginController {

    @Produces(MediaType.TEXT_HTML)
    @Get
    @View("login")
    public Object login(Principal principal) {
        if (principal != null) {
            return HttpResponse.redirect(java.net.URI.create("/"));
        }

        return Collections.<String, Object>emptyMap();
    }
}
