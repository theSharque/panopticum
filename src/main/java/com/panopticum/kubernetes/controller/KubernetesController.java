package com.panopticum.kubernetes.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.ui.AppAlerts;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.kubernetes.model.KubernetesPodInfo;
import com.panopticum.kubernetes.service.KubernetesService;
import com.panopticum.kubernetes.util.KubernetesNamespaceCsv;
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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/kubernetes")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class KubernetesController {

    private final DbConnectionService dbConnectionService;
    private final KubernetesService kubernetesService;

    @Get("/{id}")
    public HttpResponse<?> index(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/kubernetes/" + id + "/namespaces"));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces")
    @View("kubernetes/namespaces")
    public Map<String, Object> namespaces(@PathVariable Long id,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);

        Page<String> paged = kubernetesService.listNamespacesPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/pods")
    @View("kubernetes/pods")
    public Map<String, Object> pods(@PathVariable Long id,
            @PathVariable String namespace,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String ns = decodePathSegment(namespace);
        if (!isNamespaceAllowed(conn.get(), ns)) {
            AppAlerts.i18n(model, "kubernetes.namespaceNotInConnection");
            model.put("connectionId", id);
            model.put("namespace", ns);
            model.put("items", List.of());
            ControllerModelHelper.addPagination(model, Page.of(List.of(), page, size, sort, order), "items");
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/kubernetes/" + id + "/namespaces"));
        breadcrumbs.add(new BreadcrumbItem(ns, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("namespace", ns);

        Page<KubernetesPodInfo> paged = kubernetesService.listPodsPaged(id, ns, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "phase", "orderPhase"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/pods/{pod}/logs")
    @View("kubernetes/logs")
    public Map<String, Object> logs(@PathVariable Long id,
            @PathVariable String namespace,
            @PathVariable String pod,
            @QueryValue(value = "tail", defaultValue = "500") int tail) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String ns = decodePathSegment(namespace);
        String podName = decodePathSegment(pod);
        if (!isNamespaceAllowed(conn.get(), ns)) {
            AppAlerts.i18n(model, "kubernetes.namespaceNotInConnection");
            model.put("connectionId", id);
            model.put("namespace", ns);
            model.put("podName", podName);
            model.put("tail", tail);
            model.put("queryResult", QueryResult.error("kubernetes.namespaceNotAllowed"));
            return model;
        }

        int tailClamped = Math.min(Math.max(1, tail), KubernetesService.MAX_TAIL_LINES);
        QueryResult qr = kubernetesService.tailPodLogs(id, ns, podName, tailClamped);

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/kubernetes/" + id + "/namespaces"));
        breadcrumbs.add(new BreadcrumbItem(ns, "/kubernetes/" + id + "/namespaces/" + encodePathSegment(ns) + "/pods"));
        breadcrumbs.add(new BreadcrumbItem(podName, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("namespace", ns);
        model.put("podName", podName);
        model.put("tail", tailClamped);
        model.put("queryResult", qr);
        if (qr.hasError()) {
            AppAlerts.i18n(model, qr.getError());
        }

        return model;
    }

    private static boolean isNamespaceAllowed(DbConnection conn, String namespace) {
        if (namespace == null || namespace.isBlank()) {
            return false;
        }
        return KubernetesNamespaceCsv.parse(conn.getDbName()).contains(namespace);
    }

    private static String decodePathSegment(String s) {
        if (s == null || s.isBlank()) {
            return "";
        }
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }

    private static String encodePathSegment(String s) {
        if (s == null) {
            return "";
        }
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
