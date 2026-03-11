package com.panopticum.config;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

import java.net.URI;

@Controller("/swagger")
@Secured(SecurityRule.IS_ANONYMOUS)
public class SwaggerRedirectController {

    @Get
    public HttpResponse<?> redirectToSwaggerUi() {
        return HttpResponse.seeOther(URI.create("/swagger-ui/"));
    }

    @Get("/index.html")
    public HttpResponse<?> redirectToSwaggerUiIndex() {
        return HttpResponse.seeOther(URI.create("/swagger-ui/"));
    }
}
