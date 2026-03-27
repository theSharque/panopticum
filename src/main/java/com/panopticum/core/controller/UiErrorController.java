package com.panopticum.core.controller;

import com.panopticum.core.error.ConnectionUnavailableException;
import com.panopticum.core.error.MetadataAccessException;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.ui.AppAlerts;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.Produces;
import io.micronaut.views.ModelAndView;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Controller("/__panopticum-ui-error")
@RequiredArgsConstructor
public class UiErrorController {

    private static final String HX_REQUEST = "HX-Request";

    private final DbConnectionService dbConnectionService;

    @Value("${panopticum.admin-lock:false}")
    private boolean adminLock;

    @Error(global = true, exception = ConnectionUnavailableException.class)
    @Produces(MediaType.TEXT_HTML)
    public Object onConnectionUnavailable(HttpRequest<?> request, ConnectionUnavailableException e) {
        Map<String, Object> model = errorModel();
        AppAlerts.i18n(model, "error.connectionNotAvailable");
        return respondHtml(request, model);
    }

    @Error(global = true, exception = MetadataAccessException.class)
    @Produces(MediaType.TEXT_HTML)
    public Object onMetadataAccess(HttpRequest<?> request, MetadataAccessException e) {
        Map<String, Object> model = errorModel();
        String msg = e.getMessage();
        AppAlerts.fromControllerMessage(model, msg != null && !msg.isBlank() ? msg : "error.queryExecutionFailed");
        return respondHtml(request, model);
    }

    private Map<String, Object> errorModel() {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        model.put("adminLock", adminLock);
        return model;
    }

    private HttpResponse<?> respondHtml(HttpRequest<?> request, Map<String, Object> model) {
        boolean htmx = "true".equalsIgnoreCase(request.getHeaders().get(HX_REQUEST));
        if (htmx) {
            return HttpResponse.ok(new ModelAndView<>("error/htmx-app-alert", model));
        }
        return HttpResponse.ok(new ModelAndView<>("error/ui-error", model));
    }
}
