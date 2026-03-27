package com.panopticum.core.controller;

import com.panopticum.core.service.DbConnectionService;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.server.types.files.StreamedFile;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.View;
import lombok.RequiredArgsConstructor;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@Controller("/")
@Secured(SecurityRule.IS_AUTHENTICATED)
@RequiredArgsConstructor
public class DiffController {

    private static final String DIFF_JS_RESOURCE = "static/js/diff.js";

    private final DbConnectionService dbConnectionService;

    @Value("${panopticum.admin-lock:false}")
    private boolean adminLock;

    @Get("/diff")
    @View("diff/diff")
    public Map<String, Object> diffPage() {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        model.put("adminLock", adminLock);

        return model;
    }

    @Get("/diff.js")
    public HttpResponse<?> diffJs() {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(DIFF_JS_RESOURCE);
        if (stream == null) {
            return HttpResponse.notFound();
        }

        return HttpResponse.ok(new StreamedFile(stream, MediaType.of("application/javascript")))
                .contentType(MediaType.of("application/javascript"));
    }
}
