package com.panopticum.s3.controller;

import com.panopticum.core.error.AccessResult;
import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.ui.AppAlerts;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.s3.model.S3BucketInfo;
import com.panopticum.s3.model.S3ObjectInfo;
import com.panopticum.s3.service.S3Service;
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

@Controller("/s3")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class S3Controller {

    private final DbConnectionService dbConnectionService;
    private final S3Service s3Service;

    @Get("/{id}")
    public HttpResponse<?> index(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/s3/" + id + "/buckets"));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/buckets")
    @View("s3/buckets")
    public Map<String, Object> buckets(@PathVariable Long id) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = List.of(new BreadcrumbItem(conn.get().getName(), null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);

        AccessResult<List<S3BucketInfo>> result = s3Service.listBuckets(id);
        if (result.isOk()) {
            model.put("items", result.getPayload());
        } else {
            AppAlerts.i18n(model, result.getMessageKey());
            model.put("items", List.of());
        }
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/buckets/{bucket}")
    @View("s3/objects")
    public Map<String, Object> objects(@PathVariable Long id,
            @PathVariable String bucket,
            @QueryValue(value = "prefix", defaultValue = "") String prefix,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = List.of(
                new BreadcrumbItem(conn.get().getName(), "/s3/" + id + "/buckets"),
                new BreadcrumbItem(bucket, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("bucket", bucket);
        model.put("prefix", prefix);

        AccessResult<Page<S3ObjectInfo>> result = s3Service.listObjects(id, bucket, prefix, page, size);
        if (result.isOk()) {
            ControllerModelHelper.addPagination(model, result.getPayload(), "items");
        } else {
            AppAlerts.i18n(model, result.getMessageKey());
            model.put("items", List.of());
        }
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/buckets/{bucket}/peek")
    @View("s3/peek")
    public Map<String, Object> peek(@PathVariable Long id,
            @PathVariable String bucket,
            @QueryValue("key") String key,
            @QueryValue(value = "headBytes", defaultValue = "65536") int headBytes,
            @QueryValue(value = "format", defaultValue = "auto") String format) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = List.of(
                new BreadcrumbItem(conn.get().getName(), "/s3/" + id + "/buckets"),
                new BreadcrumbItem(bucket, "/s3/" + id + "/buckets/" + bucket),
                new BreadcrumbItem(key, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("bucket", bucket);
        model.put("key", key);
        model.put("headBytes", headBytes);
        model.put("format", format);

        if (key != null && !key.isBlank()) {
            AccessResult<String> result = s3Service.peekObject(id, bucket, key, headBytes, format);
            if (result.isOk()) {
                model.put("content", result.getPayload());
            } else {
                AppAlerts.i18n(model, result.getMessageKey());
            }
        }
        return model;
    }
}
