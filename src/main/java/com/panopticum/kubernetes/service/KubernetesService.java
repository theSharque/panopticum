package com.panopticum.kubernetes.service;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.kubernetes.model.KubernetesPodInfo;
import com.panopticum.kubernetes.util.KubernetesNamespaceCsv;
import io.fabric8.kubernetes.api.model.Pod;
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
import java.util.Optional;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class KubernetesService {

    public static final int MAX_TAIL_LINES = 2000;
    public static final int DEFAULT_TAIL_LINES = 100;

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
        int tail = parseTailFromQuery(query, limit);
        QueryResult qr = tailPodLogs(connectionId, ns, pod, tail);
        if (qr.hasError()) {
            return qr;
        }
        return applyOffsetLimit(qr, offset, limit);
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

    private static int parseTailFromQuery(String query, int limitCap) {
        if (query == null || query.isBlank()) {
            return Math.min(DEFAULT_TAIL_LINES, limitCap);
        }
        String q = query.trim();
        try {
            int n = Integer.parseInt(q);
            return Math.min(Math.max(1, n), Math.min(MAX_TAIL_LINES, limitCap));
        } catch (NumberFormatException ignored) {
            return Math.min(DEFAULT_TAIL_LINES, limitCap);
        }
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
                return Optional.of("kubernetes.namespaceForbidden");
            }
            if (e.getCode() == 404) {
                return Optional.of("kubernetes.namespaceNotFound");
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
}
