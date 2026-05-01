package com.panopticum.prometheus.controller;

import com.panopticum.core.error.AccessResult;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.ui.AppAlerts;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.prometheus.model.PromMetricInfo;
import com.panopticum.prometheus.service.PrometheusService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.View;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/prometheus")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class PrometheusController {

    private final DbConnectionService dbConnectionService;
    private final PrometheusService prometheusService;

    @Get("/{id}")
    public HttpResponse<?> index(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/prometheus/" + id + "/metrics"));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/metrics")
    @View("prometheus/metrics")
    public Map<String, Object> metrics(@PathVariable Long id,
            @QueryValue(value = "job", defaultValue = "") String job,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        model.put("connectionId", id);
        model.put("selectedJob", job);

        AccessResult<List<String>> jobsResult = prometheusService.listJobs(id);
        if (jobsResult.isOk()) {
            model.put("jobs", jobsResult.getPayload());
        } else {
            AppAlerts.i18n(model, jobsResult.getMessageKey());
            model.put("jobs", List.of());
        }

        AccessResult<Page<PromMetricInfo>> metricsResult = prometheusService.listMetrics(id, job.isBlank() ? null : job, page, size);
        if (metricsResult.isOk()) {
            ControllerModelHelper.addPagination(model, metricsResult.getPayload(), "metrics");
        } else {
            AppAlerts.i18n(model, metricsResult.getMessageKey());
            model.put("metrics", List.of());
        }

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/query")
    @View("prometheus/query")
    public Map<String, Object> queryPage(@PathVariable Long id,
            @QueryValue(value = "promql", defaultValue = "") String promql,
            @QueryValue(value = "mode", defaultValue = "instant") String mode,
            @QueryValue(value = "start", defaultValue = "") String start,
            @QueryValue(value = "end", defaultValue = "") String end,
            @QueryValue(value = "step", defaultValue = "60") String step) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        model.put("connectionId", id);
        model.put("promql", promql);
        model.put("mode", mode);
        model.put("start", start);
        model.put("end", end);
        model.put("step", step);

        if (!promql.isBlank()) {
            AccessResult<QueryResult> result;
            if ("range".equals(mode)) {
                result = prometheusService.executeRange(id, promql, start, end, step);
            } else {
                result = prometheusService.executeInstant(id, promql);
            }
            if (result.isOk()) {
                model.put("queryResult", result.getPayload());
            } else {
                AppAlerts.i18n(model, result.getMessageKey());
            }
        }

        return model;
    }
}
