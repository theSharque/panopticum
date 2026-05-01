package com.panopticum.kubernetes.service;

import com.panopticum.core.error.AccessResult;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.kubernetes.model.KubernetesConfigMapInfo;
import com.panopticum.kubernetes.model.KubernetesDeploymentInfo;
import com.panopticum.kubernetes.model.KubernetesEventInfo;
import com.panopticum.kubernetes.model.KubernetesIngressInfo;
import com.panopticum.kubernetes.model.KubernetesPodDescription;
import com.panopticum.kubernetes.model.KubernetesPodInfo;
import com.panopticum.kubernetes.model.KubernetesSecretInfo;
import com.panopticum.kubernetes.model.KubernetesServiceInfo;
import com.panopticum.kubernetes.model.KubernetesStatefulSetInfo;
import com.panopticum.kubernetes.util.KubernetesNamespaceCsv;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class KubernetesService {

    public static final int MAX_TAIL_LINES = 10000;

    private final DbConnectionService dbConnectionService;

    public Optional<String> testConnection(String host, Integer port, String database, String password) {
        List<String> namespaces = KubernetesNamespaceCsv.parse(database);
        if (namespaces.isEmpty()) {
            return Optional.of("kubernetes.namespaceCsvRequired");
        }
        String masterUrl = resolveMasterUrl(host, port);
        if (masterUrl == null || masterUrl.isBlank()) {
            return Optional.of("kubernetes.serverUrlRequired");
        }
        if (password == null || password.isBlank()) {
            return Optional.of("kubernetes.tokenRequired");
        }
        try (KubernetesClient client = newClient(masterUrl, password)) {
            for (String ns : namespaces) {
                Optional<String> err = verifyNamespaceAccessible(client, ns);
                if (err.isPresent()) {
                    return err;
                }
            }
        } catch (KubernetesClientException e) {
            log.warn("Kubernetes test failed: {}", e.getMessage());
            return Optional.of(kubernetesErrorKey(e));
        } catch (Exception e) {
            log.warn("Kubernetes test failed", e);
            return Optional.of("kubernetes.connectionFailed");
        }
        return Optional.empty();
    }

    public Optional<String> testConnection(Long connectionId) {
        Optional<DbConnection> conn = dbConnectionService.findById(connectionId);
        if (conn.isEmpty()) {
            return Optional.of("connection.notFound");
        }
        DbConnection c = conn.get();
        return testConnection(c.getHost(), c.getPort(), c.getDbName(), c.getPassword());
    }

    public Page<String> listNamespacesPaged(Long connectionId, int page, int size, String sort, String order) {
        Optional<DbConnection> conn = dbConnectionService.findById(connectionId);
        if (conn.isEmpty()) {
            return Page.of(List.of(), page, size, sort, order);
        }
        List<String> namespaces = KubernetesNamespaceCsv.parse(conn.get().getDbName());
        List<String> sorted = sortStrings(namespaces, sort, order);
        return Page.of(sorted, page, size, sort, order);
    }

    public Page<KubernetesPodInfo> listPodsPaged(Long connectionId, String namespace, int page, int size,
                                                 String sort, String order) {
        Optional<DbConnection> conn = dbConnectionService.findById(connectionId);
        if (conn.isEmpty() || namespace == null || namespace.isBlank()) {
            return Page.of(List.of(), page, size, sort, order);
        }
        DbConnection c = conn.get();
        if (!KubernetesNamespaceCsv.parse(c.getDbName()).contains(namespace)) {
            return Page.of(List.of(), page, size, sort, order);
        }
        String masterUrl = resolveMasterUrl(c.getHost(), c.getPort());
        if (masterUrl == null || c.getPassword() == null || c.getPassword().isBlank()) {
            return Page.of(List.of(), page, size, sort, order);
        }
        try (KubernetesClient client = newClient(masterUrl, c.getPassword())) {
            List<Pod> pods = client.pods().inNamespace(namespace).list().getItems();
            List<KubernetesPodInfo> infos = new ArrayList<>();
            for (Pod p : pods) {
                String name = p.getMetadata() != null ? p.getMetadata().getName() : null;
                if (name == null || name.isBlank()) {
                    continue;
                }
                String phase = p.getStatus() != null && p.getStatus().getPhase() != null
                        ? p.getStatus().getPhase()
                        : "";
                infos.add(KubernetesPodInfo.builder()
                        .name(name)
                        .namespace(namespace)
                        .phase(phase)
                        .build());
            }
            Comparator<KubernetesPodInfo> cmp = podComparator(sort, order);
            infos.sort(cmp);
            return Page.of(infos, page, size, sort, order);
        } catch (KubernetesClientException e) {
            log.warn("listPods failed: {}", e.getMessage());
            return Page.of(List.of(), page, size, sort, order);
        }
    }

    public QueryResult tailPodLogs(Long connectionId, String namespace, String podName, int tailLines) {
        int tail = Math.min(Math.max(1, tailLines), MAX_TAIL_LINES);
        Optional<DbConnection> conn = dbConnectionService.findById(connectionId);
        if (conn.isEmpty()) {
            return QueryResult.error("connection.notFound");
        }
        DbConnection c = conn.get();
        if (namespace == null || namespace.isBlank() || podName == null || podName.isBlank()) {
            return QueryResult.error("kubernetes.namespaceAndPodRequired");
        }
        if (!KubernetesNamespaceCsv.parse(c.getDbName()).contains(namespace)) {
            return QueryResult.error("kubernetes.namespaceNotAllowed");
        }
        String masterUrl = resolveMasterUrl(c.getHost(), c.getPort());
        if (masterUrl == null || c.getPassword() == null || c.getPassword().isBlank()) {
            return QueryResult.error("kubernetes.connectionInvalid");
        }
        try (KubernetesClient client = newClient(masterUrl, c.getPassword())) {
            Pod pod = client.pods().inNamespace(namespace).withName(podName).get();
            if (pod == null) {
                return QueryResult.error("kubernetes.podNotFound");
            }
            String container = defaultContainerName(pod);
            String log;
            if (container != null && !container.isBlank()) {
                log = client.pods().inNamespace(namespace).withName(podName).inContainer(container)
                        .tailingLines(tail).getLog();
            } else {
                log = client.pods().inNamespace(namespace).withName(podName).tailingLines(tail).getLog();
            }
            if (log == null) {
                log = "";
            }
            return logsToQueryResult(log, 0, tail, false);
        } catch (KubernetesClientException e) {
            log.warn("tailPodLogs failed: {}", e.getMessage());
            return QueryResult.error(kubernetesErrorKey(e));
        }
    }

    public QueryResult tailPodLogsForMcp(Long connectionId, String catalog, String entity, String query,
                                         int offset, int limit) {
        String ns = catalog != null ? catalog.trim() : "";
        String pod = entity != null ? entity.trim() : "";
        int safeOffset = Math.max(0, offset);
        int safeLimit = Math.max(1, limit);
        int tail = query != null && !query.isBlank()
                ? MAX_TAIL_LINES
                : Math.min(MAX_TAIL_LINES, safeOffset + safeLimit);
        QueryResult filtered = tailPodLogsWithSubstring(connectionId, ns, pod, tail, query);
        if (filtered.hasError()) {
            return filtered;
        }
        return applyOffsetLimit(filtered, safeOffset, safeLimit);
    }

    public QueryResult tailPodLogsWithSubstring(Long connectionId, String namespace, String podName,
                                                int tailLines, String substring) {
        int effectiveTail = substring != null && !substring.isBlank()
                ? MAX_TAIL_LINES
                : tailLines;
        QueryResult qr = tailPodLogs(connectionId, namespace, podName, effectiveTail);
        if (qr.hasError()) {
            return qr;
        }
        QueryResult filtered = applyQueryFilter(qr, substring);
        return applyOffsetLimit(filtered, 0, Math.max(1, tailLines));
    }

    private static QueryResult applyOffsetLimit(QueryResult qr, int offset, int limit) {
        List<List<Object>> rows = qr.getRows();
        if (offset <= 0 && rows.size() <= limit) {
            return qr;
        }
        int from = Math.min(Math.max(0, offset), rows.size());
        int to = Math.min(from + Math.max(1, limit), rows.size());
        List<List<Object>> slice = rows.subList(from, to);
        boolean hasMore = to < rows.size();
        return new QueryResult(qr.getColumns(), qr.getColumnTypes(), slice, qr.getDocIds(), null,
                offset, slice.size(), hasMore);
    }

    private static QueryResult applyQueryFilter(QueryResult qr, String query) {
        if (query == null || query.isBlank()) {
            return qr;
        }

        String needle = query.toLowerCase(Locale.ROOT);
        List<List<Object>> filteredRows = qr.getRows().stream()
                .filter(row -> row.size() > 1
                        && row.get(1) != null
                        && row.get(1).toString().toLowerCase(Locale.ROOT).contains(needle))
                .collect(Collectors.toList());

        return new QueryResult(
                qr.getColumns(),
                qr.getColumnTypes(),
                filteredRows,
                qr.getDocIds(),
                null,
                0,
                filteredRows.size(),
                false
        );
    }

    private static String defaultContainerName(Pod pod) {
        if (pod.getSpec() == null || pod.getSpec().getContainers() == null
                || pod.getSpec().getContainers().isEmpty()) {
            return null;
        }
        return pod.getSpec().getContainers().get(0).getName();
    }

    private static QueryResult logsToQueryResult(String log, int offset, int limit, boolean hasMore) {
        List<String> lines = new ArrayList<>(splitLines(log));
        Collections.reverse(lines);
        List<List<Object>> rows = new ArrayList<>();
        int lineNo = 1;
        for (String line : lines) {
            rows.add(List.of(lineNo, line));
            lineNo++;
        }
        List<String> columns = List.of("line", "message");
        return new QueryResult(columns, List.of("integer", "string"), rows, null, null, offset, limit, hasMore);
    }

    private static List<String> splitLines(String log) {
        if (log == null || log.isEmpty()) {
            return List.of();
        }
        String normalized = log.endsWith("\n") ? log.substring(0, log.length() - 1) : log;
        if (normalized.isEmpty()) {
            return List.of();
        }
        return List.of(normalized.split("\n", -1));
    }

    private static Comparator<KubernetesPodInfo> podComparator(String sort, String order) {
        String s = sort != null ? sort : "name";
        boolean desc = "desc".equalsIgnoreCase(order);
        Comparator<KubernetesPodInfo> base = switch (s.toLowerCase(Locale.ROOT)) {
            case "phase" -> Comparator.comparing(p -> p.getPhase() != null ? p.getPhase() : "",
                    String.CASE_INSENSITIVE_ORDER);
            default -> Comparator.comparing(p -> p.getName() != null ? p.getName() : "", String.CASE_INSENSITIVE_ORDER);
        };
        if (desc) {
            base = base.reversed();
        }
        return base.thenComparing(p -> p.getName() != null ? p.getName() : "", String.CASE_INSENSITIVE_ORDER);
    }

    private static List<String> sortStrings(List<String> items, String sort, String order) {
        List<String> copy = new ArrayList<>(items);
        boolean desc = "desc".equalsIgnoreCase(order);
        Comparator<String> cmp = String.CASE_INSENSITIVE_ORDER;
        copy.sort(desc ? cmp.reversed() : cmp);
        return copy;
    }

    private static Optional<String> verifyNamespaceAccessible(KubernetesClient client, String namespace) {
        try {
            client.pods().inNamespace(namespace).list();
            return Optional.empty();
        } catch (KubernetesClientException e) {
            if (e.getCode() == 403) {
                return Optional.of("kubernetes.namespaceForbiddenWithName|" + namespace);
            }
            if (e.getCode() == 404) {
                return Optional.of("kubernetes.namespaceNotFoundWithName|" + namespace);
            }
            return Optional.of(kubernetesErrorKey(e));
        }
    }

    private static String kubernetesErrorKey(KubernetesClientException e) {
        if (e.getCode() == 401) {
            return "kubernetes.unauthorized";
        }
        if (e.getCode() == 403) {
            return "kubernetes.forbidden";
        }
        return "kubernetes.clientError";
    }

    static String resolveMasterUrl(String host, Integer port) {
        if (host == null || host.isBlank()) {
            return null;
        }
        String h = host.trim();
        if (h.startsWith("http://") || h.startsWith("https://")) {
            return h;
        }
        int p = port != null && port > 0 ? port : 443;
        return "https://" + h + (p != 443 ? ":" + p : "");
    }

    static KubernetesClient newClient(String masterUrl, String token) {
        Config config = new ConfigBuilder()
                .withMasterUrl(masterUrl)
                .withOauthToken(token)
                .withTrustCerts(true)
                .build();
        return new KubernetesClientBuilder().withConfig(config).build();
    }

    public AccessResult<KubernetesPodDescription> describePod(Long connectionId, String ns, String podName) {
        return withClient(connectionId, client -> {
            Pod pod = client.pods().inNamespace(ns).withName(podName).get();
            if (pod == null) {
                return AccessResult.notFound("kubernetes.access.notFound");
            }
            List<KubernetesPodDescription.ContainerInfo> containers = new ArrayList<>();
            if (pod.getStatus() != null && pod.getStatus().getContainerStatuses() != null) {
                for (var cs : pod.getStatus().getContainerStatuses()) {
                    String image = cs.getImage();
                    if (image == null && pod.getSpec() != null) {
                        image = pod.getSpec().getContainers().stream()
                                .filter(c -> cs.getName().equals(c.getName()))
                                .map(c -> c.getImage())
                                .findFirst().orElse("");
                    }
                    containers.add(KubernetesPodDescription.ContainerInfo.builder()
                            .name(cs.getName())
                            .image(image != null ? image : "")
                            .ready(Boolean.TRUE.equals(cs.getReady()))
                            .restartCount(cs.getRestartCount() != null ? cs.getRestartCount() : 0)
                            .build());
                }
            }
            List<Map<String, Object>> conditions = new ArrayList<>();
            if (pod.getStatus() != null && pod.getStatus().getConditions() != null) {
                for (var cond : pod.getStatus().getConditions()) {
                    conditions.add(Map.of(
                            "type", cond.getType() != null ? cond.getType() : "",
                            "status", cond.getStatus() != null ? cond.getStatus() : "",
                            "message", cond.getMessage() != null ? cond.getMessage() : ""));
                }
            }
            List<KubernetesEventInfo> recentEvents = fetchPodEvents(client, ns, podName);
            KubernetesPodDescription desc = KubernetesPodDescription.builder()
                    .name(pod.getMetadata().getName())
                    .namespace(ns)
                    .phase(pod.getStatus() != null ? pod.getStatus().getPhase() : "Unknown")
                    .nodeName(pod.getSpec() != null ? pod.getSpec().getNodeName() : null)
                    .podIP(pod.getStatus() != null ? pod.getStatus().getPodIP() : null)
                    .containers(containers)
                    .conditions(conditions)
                    .recentEvents(recentEvents)
                    .build();
            return AccessResult.ok(desc);
        });
    }

    private List<KubernetesEventInfo> fetchPodEvents(KubernetesClient client, String ns, String podName) {
        try {
            return client.v1().events().inNamespace(ns).list().getItems().stream()
                    .filter(e -> e.getInvolvedObject() != null && podName.equals(e.getInvolvedObject().getName()))
                    .map(KubernetesService::toEventInfo)
                    .toList();
        } catch (Exception e) {
            return List.of();
        }
    }

    public AccessResult<Page<KubernetesEventInfo>> listEventsPaged(Long connectionId, String ns, int page, int size) {
        return withClient(connectionId, client -> {
            List<KubernetesEventInfo> events = client.v1().events().inNamespace(ns).list().getItems().stream()
                    .map(KubernetesService::toEventInfo)
                    .toList();
            return AccessResult.ok(Page.of(events, page, size, "name", "asc"));
        });
    }

    public AccessResult<Page<KubernetesDeploymentInfo>> listDeploymentsPaged(Long connectionId, String ns, int page, int size) {
        return withClient(connectionId, client -> {
            List<KubernetesDeploymentInfo> items = client.apps().deployments().inNamespace(ns).list().getItems().stream()
                    .map(KubernetesService::toDeploymentInfo)
                    .toList();
            return AccessResult.ok(Page.of(items, page, size, "name", "asc"));
        });
    }

    public AccessResult<Page<KubernetesStatefulSetInfo>> listStatefulSetsPaged(Long connectionId, String ns, int page, int size) {
        return withClient(connectionId, client -> {
            List<KubernetesStatefulSetInfo> items = client.apps().statefulSets().inNamespace(ns).list().getItems().stream()
                    .map(KubernetesService::toStatefulSetInfo)
                    .toList();
            return AccessResult.ok(Page.of(items, page, size, "name", "asc"));
        });
    }

    public AccessResult<Page<KubernetesServiceInfo>> listServicesPaged(Long connectionId, String ns, int page, int size) {
        return withClient(connectionId, client -> {
            List<KubernetesServiceInfo> items = client.services().inNamespace(ns).list().getItems().stream()
                    .map(KubernetesService::toServiceInfo)
                    .toList();
            return AccessResult.ok(Page.of(items, page, size, "name", "asc"));
        });
    }

    public AccessResult<Page<KubernetesIngressInfo>> listIngressesPaged(Long connectionId, String ns, int page, int size) {
        return withClient(connectionId, client -> {
            List<KubernetesIngressInfo> items = client.network().v1().ingresses().inNamespace(ns).list().getItems().stream()
                    .map(KubernetesService::toIngressInfo)
                    .toList();
            return AccessResult.ok(Page.of(items, page, size, "name", "asc"));
        });
    }

    public AccessResult<Page<KubernetesConfigMapInfo>> listConfigMapsPaged(Long connectionId, String ns, int page, int size) {
        return withClient(connectionId, client -> {
            List<KubernetesConfigMapInfo> items = client.configMaps().inNamespace(ns).list().getItems().stream()
                    .map(KubernetesService::toConfigMapInfo)
                    .toList();
            return AccessResult.ok(Page.of(items, page, size, "name", "asc"));
        });
    }

    public AccessResult<Page<KubernetesSecretInfo>> listSecretsPaged(Long connectionId, String ns, int page, int size) {
        return withClient(connectionId, client -> {
            List<KubernetesSecretInfo> items = client.secrets().inNamespace(ns).list().getItems().stream()
                    .map(KubernetesService::toSecretInfo)
                    .toList();
            return AccessResult.ok(Page.of(items, page, size, "name", "asc"));
        });
    }

    public AccessResult<String> getSecretValue(Long connectionId, String ns, String secretName, String key) {
        return withClient(connectionId, client -> {
            Secret secret = client.secrets().inNamespace(ns).withName(secretName).get();
            if (secret == null) {
                return AccessResult.notFound("kubernetes.access.notFound");
            }
            if (secret.getData() == null || !secret.getData().containsKey(key)) {
                return AccessResult.notFound("kubernetes.access.notFound");
            }
            String base64Value = secret.getData().get(key);
            String decoded = new String(java.util.Base64.getDecoder().decode(base64Value));
            log.info("Secret value revealed: connection={} namespace={} secret={} key={}", connectionId, ns, secretName, key);
            return AccessResult.ok(decoded);
        });
    }

    private <T> AccessResult<T> withClient(Long connectionId, java.util.function.Function<KubernetesClient, AccessResult<T>> action) {
        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId);
        if (connOpt.isEmpty()) {
            return AccessResult.notFound("connection.notFound");
        }
        DbConnection conn = connOpt.get();
        String masterUrl = resolveMasterUrl(conn.getHost(), conn.getPort());
        if (masterUrl == null) {
            return AccessResult.error("kubernetes.access.error");
        }
        try (KubernetesClient client = newClient(masterUrl, conn.getPassword())) {
            return action.apply(client);
        } catch (KubernetesClientException e) {
            log.warn("Kubernetes access error code={}: {}", e.getCode(), e.getMessage());
            return switch (e.getCode()) {
                case 401 -> AccessResult.unauthorized("kubernetes.access.unauthorized");
                case 403 -> AccessResult.forbidden("kubernetes.access.forbidden");
                case 404 -> AccessResult.notFound("kubernetes.access.notFound");
                default -> AccessResult.error("kubernetes.access.error");
            };
        } catch (Exception e) {
            log.warn("Kubernetes access error: {}", e.getMessage());
            return AccessResult.error("kubernetes.access.error");
        }
    }

    private static KubernetesEventInfo toEventInfo(Event e) {
        return KubernetesEventInfo.builder()
                .name(e.getMetadata() != null ? e.getMetadata().getName() : "")
                .namespace(e.getMetadata() != null ? e.getMetadata().getNamespace() : "")
                .reason(e.getReason() != null ? e.getReason() : "")
                .message(e.getMessage() != null ? e.getMessage() : "")
                .objectName(e.getInvolvedObject() != null ? e.getInvolvedObject().getName() : "")
                .objectKind(e.getInvolvedObject() != null ? e.getInvolvedObject().getKind() : "")
                .count(e.getCount() != null ? e.getCount() : 0)
                .firstTime(e.getFirstTimestamp() != null ? e.getFirstTimestamp() : "")
                .lastTime(e.getLastTimestamp() != null ? e.getLastTimestamp() : "")
                .type(e.getType() != null ? e.getType() : "Normal")
                .build();
    }

    private static KubernetesDeploymentInfo toDeploymentInfo(Deployment d) {
        String image = "";
        if (d.getSpec() != null && d.getSpec().getTemplate() != null
                && d.getSpec().getTemplate().getSpec() != null
                && !d.getSpec().getTemplate().getSpec().getContainers().isEmpty()) {
            image = d.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();
        }
        return KubernetesDeploymentInfo.builder()
                .name(d.getMetadata().getName())
                .namespace(d.getMetadata().getNamespace())
                .desiredReplicas(d.getSpec() != null && d.getSpec().getReplicas() != null ? d.getSpec().getReplicas() : 0)
                .readyReplicas(d.getStatus() != null && d.getStatus().getReadyReplicas() != null ? d.getStatus().getReadyReplicas() : 0)
                .availableReplicas(d.getStatus() != null && d.getStatus().getAvailableReplicas() != null ? d.getStatus().getAvailableReplicas() : 0)
                .image(image != null ? image : "")
                .build();
    }

    private static KubernetesStatefulSetInfo toStatefulSetInfo(StatefulSet ss) {
        String image = "";
        if (ss.getSpec() != null && ss.getSpec().getTemplate() != null
                && ss.getSpec().getTemplate().getSpec() != null
                && !ss.getSpec().getTemplate().getSpec().getContainers().isEmpty()) {
            image = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();
        }
        return KubernetesStatefulSetInfo.builder()
                .name(ss.getMetadata().getName())
                .namespace(ss.getMetadata().getNamespace())
                .desiredReplicas(ss.getSpec() != null && ss.getSpec().getReplicas() != null ? ss.getSpec().getReplicas() : 0)
                .readyReplicas(ss.getStatus() != null && ss.getStatus().getReadyReplicas() != null ? ss.getStatus().getReadyReplicas() : 0)
                .image(image != null ? image : "")
                .build();
    }

    private static KubernetesServiceInfo toServiceInfo(io.fabric8.kubernetes.api.model.Service svc) { //NOSONAR - fabric8 Service, not spring Service
        String ports = "";
        if (svc.getSpec() != null && svc.getSpec().getPorts() != null) {
            ports = svc.getSpec().getPorts().stream()
                    .map(p -> (p.getName() != null ? p.getName() + ":" : "") + p.getPort()
                            + (p.getProtocol() != null ? "/" + p.getProtocol() : ""))
                    .collect(Collectors.joining(", "));
        }
        return KubernetesServiceInfo.builder()
                .name(svc.getMetadata().getName())
                .namespace(svc.getMetadata().getNamespace())
                .type(svc.getSpec() != null ? svc.getSpec().getType() : "")
                .clusterIP(svc.getSpec() != null ? svc.getSpec().getClusterIP() : "")
                .ports(ports)
                .build();
    }

    private static KubernetesIngressInfo toIngressInfo(Ingress ing) {
        List<String> hosts = new ArrayList<>();
        if (ing.getSpec() != null && ing.getSpec().getRules() != null) {
            for (var rule : ing.getSpec().getRules()) {
                if (rule.getHost() != null) hosts.add(rule.getHost());
            }
        }
        String className = ing.getSpec() != null ? ing.getSpec().getIngressClassName() : null;
        return KubernetesIngressInfo.builder()
                .name(ing.getMetadata().getName())
                .namespace(ing.getMetadata().getNamespace())
                .hosts(hosts)
                .className(className != null ? className : "")
                .build();
    }

    private static KubernetesConfigMapInfo toConfigMapInfo(ConfigMap cm) {
        List<String> keys = new ArrayList<>();
        if (cm.getData() != null) keys.addAll(cm.getData().keySet());
        if (cm.getBinaryData() != null) keys.addAll(cm.getBinaryData().keySet());
        return KubernetesConfigMapInfo.builder()
                .name(cm.getMetadata().getName())
                .namespace(cm.getMetadata().getNamespace())
                .keys(keys)
                .build();
    }

    private static KubernetesSecretInfo toSecretInfo(Secret sec) {
        List<String> keys = new ArrayList<>();
        if (sec.getData() != null) keys.addAll(sec.getData().keySet());
        return KubernetesSecretInfo.builder()
                .name(sec.getMetadata().getName())
                .namespace(sec.getMetadata().getNamespace())
                .type(sec.getType() != null ? sec.getType() : "")
                .keys(keys)
                .build();
    }
}
