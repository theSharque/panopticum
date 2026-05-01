package com.panopticum.kubernetes.controller;

import com.panopticum.core.error.AccessResult;
import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.ui.AppAlerts;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.kubernetes.model.KubernetesConfigMapInfo;
import com.panopticum.kubernetes.model.KubernetesDeploymentInfo;
import com.panopticum.kubernetes.model.KubernetesEventInfo;
import com.panopticum.kubernetes.model.KubernetesIngressInfo;
import com.panopticum.kubernetes.model.KubernetesPodDescription;
import com.panopticum.kubernetes.model.KubernetesPodInfo;
import com.panopticum.kubernetes.model.KubernetesSecretInfo;
import com.panopticum.kubernetes.model.KubernetesServiceInfo;
import com.panopticum.kubernetes.model.KubernetesStatefulSetInfo;
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
            @QueryValue(value = "tail", defaultValue = "500") int tail,
            @QueryValue(value = "substring", defaultValue = "") String substring) {
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
            model.put("substring", substring);
            model.put("queryResult", QueryResult.error("kubernetes.namespaceNotAllowed"));
            return model;
        }

        int tailClamped = Math.min(Math.max(1, tail), KubernetesService.MAX_TAIL_LINES);
        QueryResult qr = kubernetesService.tailPodLogsWithSubstring(id, ns, podName, tailClamped, substring);

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/kubernetes/" + id + "/namespaces"));
        breadcrumbs.add(new BreadcrumbItem(ns, "/kubernetes/" + id + "/namespaces/" + encodePathSegment(ns) + "/pods"));
        breadcrumbs.add(new BreadcrumbItem(podName, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("namespace", ns);
        model.put("podName", podName);
        model.put("tail", tailClamped);
        model.put("substring", substring);
        model.put("queryResult", qr);
        if (qr.hasError()) {
            AppAlerts.i18n(model, qr.getError());
        }

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/pods/{pod}")
    @View("kubernetes/pod")
    public Map<String, Object> pod(@PathVariable Long id,
            @PathVariable String namespace,
            @PathVariable String pod) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        String ns = decodePathSegment(namespace);
        String podName = decodePathSegment(pod);
        if (!isNamespaceAllowed(conn.get(), ns)) {
            AppAlerts.i18n(model, "kubernetes.namespaceNotInConnection");
            return model;
        }
        List<BreadcrumbItem> breadcrumbs = List.of(
                new BreadcrumbItem(conn.get().getName(), "/kubernetes/" + id + "/namespaces"),
                new BreadcrumbItem(ns, "/kubernetes/" + id + "/namespaces/" + encodePathSegment(ns) + "/pods"),
                new BreadcrumbItem(podName, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("namespace", ns);
        model.put("podName", podName);
        AccessResult<KubernetesPodDescription> result = kubernetesService.describePod(id, ns, podName);
        if (!result.isOk()) {
            AppAlerts.i18n(model, result.getMessageKey());
        } else {
            model.put("podDesc", result.getPayload());
        }
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/events")
    @View("kubernetes/events")
    public Map<String, Object> events(@PathVariable Long id,
            @PathVariable String namespace,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "100") int size) {
        Map<String, Object> model = buildNsModel(id, namespace, "events", page, size, null, null);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty() || !isNamespaceAllowed(conn.get(), decodePathSegment(namespace))) {
            return model;
        }
        String ns = decodePathSegment(namespace);
        AccessResult<Page<KubernetesEventInfo>> result = kubernetesService.listEventsPaged(id, ns, page, size);
        applyAccessResult(model, result, "items");
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/deployments")
    @View("kubernetes/deployments")
    public Map<String, Object> deployments(@PathVariable Long id,
            @PathVariable String namespace,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size) {
        Map<String, Object> model = buildNsModel(id, namespace, "deployments", page, size, null, null);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty() || !isNamespaceAllowed(conn.get(), decodePathSegment(namespace))) {
            return model;
        }
        String ns = decodePathSegment(namespace);
        AccessResult<Page<KubernetesDeploymentInfo>> result = kubernetesService.listDeploymentsPaged(id, ns, page, size);
        applyAccessResult(model, result, "items");
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/statefulsets")
    @View("kubernetes/statefulsets")
    public Map<String, Object> statefulsets(@PathVariable Long id,
            @PathVariable String namespace,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size) {
        Map<String, Object> model = buildNsModel(id, namespace, "statefulsets", page, size, null, null);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty() || !isNamespaceAllowed(conn.get(), decodePathSegment(namespace))) {
            return model;
        }
        String ns = decodePathSegment(namespace);
        AccessResult<Page<KubernetesStatefulSetInfo>> result = kubernetesService.listStatefulSetsPaged(id, ns, page, size);
        applyAccessResult(model, result, "items");
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/services")
    @View("kubernetes/services")
    public Map<String, Object> services(@PathVariable Long id,
            @PathVariable String namespace,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size) {
        Map<String, Object> model = buildNsModel(id, namespace, "services", page, size, null, null);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty() || !isNamespaceAllowed(conn.get(), decodePathSegment(namespace))) {
            return model;
        }
        String ns = decodePathSegment(namespace);
        AccessResult<Page<KubernetesServiceInfo>> result = kubernetesService.listServicesPaged(id, ns, page, size);
        applyAccessResult(model, result, "items");
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/ingresses")
    @View("kubernetes/ingresses")
    public Map<String, Object> ingresses(@PathVariable Long id,
            @PathVariable String namespace,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size) {
        Map<String, Object> model = buildNsModel(id, namespace, "ingresses", page, size, null, null);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty() || !isNamespaceAllowed(conn.get(), decodePathSegment(namespace))) {
            return model;
        }
        String ns = decodePathSegment(namespace);
        AccessResult<Page<KubernetesIngressInfo>> result = kubernetesService.listIngressesPaged(id, ns, page, size);
        applyAccessResult(model, result, "items");
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/configmaps")
    @View("kubernetes/configmaps")
    public Map<String, Object> configmaps(@PathVariable Long id,
            @PathVariable String namespace,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size) {
        Map<String, Object> model = buildNsModel(id, namespace, "configmaps", page, size, null, null);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty() || !isNamespaceAllowed(conn.get(), decodePathSegment(namespace))) {
            return model;
        }
        String ns = decodePathSegment(namespace);
        AccessResult<Page<KubernetesConfigMapInfo>> result = kubernetesService.listConfigMapsPaged(id, ns, page, size);
        applyAccessResult(model, result, "items");
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/secrets")
    @View("kubernetes/secrets")
    public Map<String, Object> secrets(@PathVariable Long id,
            @PathVariable String namespace,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size) {
        Map<String, Object> model = buildNsModel(id, namespace, "secrets", page, size, null, null);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty() || !isNamespaceAllowed(conn.get(), decodePathSegment(namespace))) {
            return model;
        }
        String ns = decodePathSegment(namespace);
        AccessResult<Page<KubernetesSecretInfo>> result = kubernetesService.listSecretsPaged(id, ns, page, size);
        applyAccessResult(model, result, "items");
        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/namespaces/{namespace}/secrets/{secretName}/value")
    public HttpResponse<String> secretValue(@PathVariable Long id,
            @PathVariable String namespace,
            @PathVariable String secretName,
            @QueryValue("key") String key) {
        String ns = decodePathSegment(namespace);
        String name = decodePathSegment(secretName);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty() || !isNamespaceAllowed(conn.get(), ns)) {
            return HttpResponse.ok("<span class=\"secret-value secret-value--masked\">no access</span>");
        }
        AccessResult<String> result = kubernetesService.getSecretValue(id, ns, name, key);
        if (!result.isOk()) {
            return HttpResponse.ok("<span class=\"secret-value secret-value--error\">no access</span>");
        }
        String escaped = result.getPayload()
                .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
        return HttpResponse.ok(
                "<span class=\"secret-value\">" + escaped + "</span>"
                + "<button class=\"btn-copy\" onclick=\"navigator.clipboard.writeText(this.previousElementSibling.textContent)\">Copy</button>");
    }

    private Map<String, Object> buildNsModel(Long id, String namespace, String section, int page, int size, String sort, String order) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }
        String ns = decodePathSegment(namespace);
        if (!isNamespaceAllowed(conn.get(), ns)) {
            AppAlerts.i18n(model, "kubernetes.namespaceNotInConnection");
        }
        String nsUrl = "/kubernetes/" + id + "/namespaces/" + encodePathSegment(ns);
        List<BreadcrumbItem> breadcrumbs = List.of(
                new BreadcrumbItem(conn.get().getName(), "/kubernetes/" + id + "/namespaces"),
                new BreadcrumbItem(ns, nsUrl + "/pods"),
                new BreadcrumbItem(section, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("namespace", ns);
        model.put("section", section);
        model.put("items", List.of());
        if (sort != null) model.put("sort", sort);
        if (order != null) model.put("order", order);
        model.put("page", page);
        model.put("size", size);
        return model;
    }

    private <T> void applyAccessResult(Map<String, Object> model, AccessResult<Page<T>> result, String itemsKey) {
        if (result.isOk()) {
            ControllerModelHelper.addPagination(model, result.getPayload(), itemsKey);
        } else {
            AppAlerts.i18n(model, result.getMessageKey());
            model.put(itemsKey, List.of());
        }
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
