package com.panopticum.mcp.service;

import com.panopticum.core.model.ConnectionType;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.BreadcrumbPathHelper;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
@RequiredArgsConstructor
public class PanopticumLinkResolverService {

    private static final Set<String> ROUTE_MARKERS = Set.of(
            "topics", "partitions", "records", "buckets", "queues", "messages",
            "namespaces", "pods", "indices", "metrics", "collections", "scopes",
            "detail", "sql", "cql", "query", "peek", "search", "logs", "doc",
            "deployments", "events", "statefulsets", "services", "ingresses",
            "configmaps", "secrets", "value", "databases", "schemas");

    private static final List<ConnectionType> TYPES_BY_PREFIX_LENGTH = Arrays.stream(ConnectionType.values())
            .sorted(Comparator.comparingInt((ConnectionType t) -> t.getUiPathPrefix().length()).reversed())
            .toList();

    private final DbConnectionService dbConnectionService;

    public ResolveOutcome resolve(String link) {
        if (link == null || link.isBlank()) {
            return ResolveOutcome.error("link is required", dbConnectionService.listConfiguredUiPaths());
        }

        String trimmed = link.trim();
        String path = extractPath(trimmed);
        if (path == null || path.isBlank()) {
            return ResolveOutcome.error("Could not extract path from link", dbConnectionService.listConfiguredUiPaths());
        }

        String normalized = trimTrailingSlash(path);

        if (looksLikeUiPath(normalized)) {
            String uiPath = normalized.startsWith("/") ? normalized : "/" + normalized;
            return resolveUiPath(uiPath);
        }

        return resolveBreadcrumbPath(normalized);
    }

    private ResolveOutcome resolveBreadcrumbPath(String copyPath) {
        Optional<BreadcrumbPathHelper.BreadcrumbMatch> matchOpt = dbConnectionService.findByBreadcrumbCopyPath(copyPath);
        if (matchOpt.isEmpty()) {
            return ResolveOutcome.error("Connection not found for breadcrumb path: " + copyPath, dbConnectionService.listConfiguredUiPaths());
        }

        BreadcrumbPathHelper.BreadcrumbMatch match = matchOpt.get();
        DbConnection conn = match.connection();
        Optional<ConnectionType> ctOpt = ConnectionType.fromStoredType(conn.getType());
        if (ctOpt.isEmpty()) {
            return ResolveOutcome.error("Unsupported connection type: " + conn.getType(), dbConnectionService.listConfiguredUiPaths());
        }

        ConnectionType ct = ctOpt.get();
        List<String> scopeSegments = match.scopeLabels();
        String uiPath = ct.getUiPathPrefix() + "/" + conn.getId();
        if (!scopeSegments.isEmpty()) {
            uiPath = uiPath + "/" + scopeSegments.stream()
                    .map(DbConnectionService::encodeUiPathSegment)
                    .collect(Collectors.joining("/"));
        }

        Map<String, Object> resolved = buildResolved(conn, ct, scopeSegments, uiPath);
        return ResolveOutcome.success(resolved);
    }

    private boolean looksLikeUiPath(String path) {
        if (path.startsWith("/")) {
            return true;
        }

        for (ConnectionType ct : TYPES_BY_PREFIX_LENGTH) {
            String prefix = ct.getUiPathPrefix().substring(1);
            if (!path.equals(prefix) && !path.startsWith(prefix + "/")) {
                continue;
            }

            String remainder = path.length() == prefix.length()
                    ? ""
                    : path.substring(prefix.length() + 1);
            List<String> segments = splitDecodedSegments(remainder);
            if (!segments.isEmpty() && parseConnectionId(segments.get(0)) != null) {
                return true;
            }
        }

        return false;
    }

    private static String trimTrailingSlash(String path) {
        if (path.endsWith("/") && path.length() > 1) {
            return path.substring(0, path.length() - 1);
        }

        return path;
    }

    private ResolveOutcome resolveUiPath(String path) {
        String normalized = trimTrailingSlash(path);

        for (ConnectionType ct : TYPES_BY_PREFIX_LENGTH) {
            String prefix = ct.getUiPathPrefix();
            if (!normalized.equals(prefix) && !normalized.startsWith(prefix + "/")) {
                continue;
            }

            String remainder = normalized.length() == prefix.length()
                    ? ""
                    : normalized.substring(prefix.length() + 1);
            List<String> segments = splitDecodedSegments(remainder);
            if (segments.isEmpty()) {
                return ResolveOutcome.error("Missing connection id in path", dbConnectionService.listConfiguredUiPaths());
            }

            String idToken = segments.get(0);
            Long connectionId = parseConnectionId(idToken);
            if (connectionId == null) {
                return ResolveOutcome.error("Invalid connection id: " + idToken, dbConnectionService.listConfiguredUiPaths());
            }

            Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId);
            if (connOpt.isEmpty()) {
                return ResolveOutcome.error("Connection not found: " + connectionId, dbConnectionService.listConfiguredUiPaths());
            }

            DbConnection conn = connOpt.get();
            Optional<ConnectionType> storedType = ConnectionType.fromStoredType(conn.getType());
            if (storedType.isEmpty() || storedType.get() != ct) {
                return ResolveOutcome.error(
                        "Path prefix " + prefix + " does not match connection type " + conn.getType(),
                        dbConnectionService.listConfiguredUiPaths());
            }

            List<String> scopeSegments = segments.size() > 1 ? segments.subList(1, segments.size()) : List.of();
            Map<String, Object> resolved = buildResolved(conn, ct, scopeSegments, normalized);
            return ResolveOutcome.success(resolved);
        }

        return ResolveOutcome.error("Unknown Panopticum UI path: " + normalized, dbConnectionService.listConfiguredUiPaths());
    }

    private Map<String, Object> buildResolved(DbConnection conn, ConnectionType ct, List<String> scopeSegments, String uiPath) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connectionId", conn.getId());
        result.put("name", conn.getName());
        result.put("dbType", ct.getCanonicalId());
        result.put("hierarchyModel", ct.getHierarchyModel());
        result.put("uiPath", uiPath);

        applyScope(ct, stripRouteMarkers(scopeSegments), result);
        return result;
    }

    private void applyScope(ConnectionType ct, List<String> segments, Map<String, Object> result) {
        if (segments.isEmpty()) {
            return;
        }

        if (tryStructuredScope(ct, segments, result)) {
            return;
        }

        switch (ct.getHierarchyModel()) {
            case "catalog.namespace.entity" -> {
                putIfPresent(result, "catalog", segmentAt(segments, 0));
                putIfPresent(result, "namespace", segmentAt(segments, 1));
                putIfPresent(result, "entity", segmentAt(segments, 2));
            }
            case "catalog.entity" -> {
                putIfPresent(result, "catalog", segmentAt(segments, 0));
                putIfPresent(result, "entity", segmentAt(segments, 1));
            }
            case "namespace.entity" -> {
                putIfPresent(result, "namespace", segmentAt(segments, 0));
                putIfPresent(result, "entity", segmentAt(segments, 1));
            }
            case "catalog" -> putIfPresent(result, "catalog", segmentAt(segments, 0));
            default -> {
            }
        }
    }

    private boolean tryStructuredScope(ConnectionType ct, List<String> segments, Map<String, Object> result) {
        return switch (ct) {
            case KAFKA -> parseKafkaScope(segments, result);
            case RABBITMQ -> parseRabbitMqScope(segments, result);
            case KUBERNETES -> parseKubernetesScope(segments, result);
            case COUCHBASE -> parseCouchbaseScope(segments, result);
            case ELASTICSEARCH -> parseElasticsearchScope(segments, result);
            case S3 -> parseS3Scope(segments, result);
            default -> false;
        };
    }

    private boolean parseKafkaScope(List<String> segments, Map<String, Object> result) {
        if (segments.size() >= 2 && "topics".equalsIgnoreCase(segments.get(0))) {
            putIfPresent(result, "catalog", segments.get(1));
            int partitionIdx = indexOfIgnoreCase(segments, "partitions");
            if (partitionIdx >= 0 && partitionIdx + 1 < segments.size()) {
                putIfPresent(result, "entity", segments.get(partitionIdx + 1));
            }
            return true;
        }
        return false;
    }

    private boolean parseRabbitMqScope(List<String> segments, Map<String, Object> result) {
        if (segments.size() >= 2 && "queues".equalsIgnoreCase(segments.get(0))) {
            putIfPresent(result, "catalog", segments.get(1));
            if (segments.size() >= 3 && !isRouteMarker(segments.get(2))) {
                putIfPresent(result, "entity", segments.get(2));
            }
            return true;
        }
        return false;
    }

    private boolean parseKubernetesScope(List<String> segments, Map<String, Object> result) {
        if (segments.size() >= 2 && "namespaces".equalsIgnoreCase(segments.get(0))) {
            putIfPresent(result, "catalog", segments.get(1));
            int podsIdx = indexOfIgnoreCase(segments, "pods");
            if (podsIdx >= 0 && podsIdx + 1 < segments.size()) {
                putIfPresent(result, "entity", segments.get(podsIdx + 1));
            }
            return true;
        }
        return false;
    }

    private boolean parseCouchbaseScope(List<String> segments, Map<String, Object> result) {
        if (segments.isEmpty()) {
            return false;
        }
        if ("buckets".equalsIgnoreCase(segments.get(0))) {
            return true;
        }
        putIfPresent(result, "catalog", segments.get(0));
        if (segments.size() >= 2 && !"collections".equalsIgnoreCase(segments.get(1))) {
            putIfPresent(result, "namespace", segments.get(1));
        }
        if (segments.size() >= 3) {
            putIfPresent(result, "entity", segments.get(2));
        }
        return true;
    }

    private boolean parseElasticsearchScope(List<String> segments, Map<String, Object> result) {
        if (segments.size() >= 2 && "indices".equalsIgnoreCase(segments.get(0))) {
            putIfPresent(result, "catalog", segments.get(1));
            return true;
        }
        return false;
    }

    private boolean parseS3Scope(List<String> segments, Map<String, Object> result) {
        if (segments.size() >= 2 && "buckets".equalsIgnoreCase(segments.get(0))) {
            putIfPresent(result, "catalog", segments.get(1));
            return true;
        }
        return false;
    }

    private static List<String> stripRouteMarkers(List<String> segments) {
        if (segments.isEmpty()) {
            return segments;
        }
        int end = segments.size();
        while (end > 0 && isRouteMarker(segments.get(end - 1))) {
            end--;
        }
        if (end == segments.size()) {
            return segments;
        }
        return new ArrayList<>(segments.subList(0, end));
    }

    private static boolean isRouteMarker(String segment) {
        return ROUTE_MARKERS.contains(segment.toLowerCase(Locale.ROOT));
    }

    private static int indexOfIgnoreCase(List<String> segments, String marker) {
        for (int i = 0; i < segments.size(); i++) {
            if (marker.equalsIgnoreCase(segments.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private static void putIfPresent(Map<String, Object> result, String key, String value) {
        if (value != null && !value.isBlank()) {
            result.put(key, value);
        }
    }

    private static String segmentAt(List<String> segments, int index) {
        return index < segments.size() ? segments.get(index) : null;
    }

    private static Long parseConnectionId(String token) {
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String extractPath(String input) {
        if (input.contains("://")) {
            try {
                URI uri = URI.create(input);
                String p = uri.getPath();
                return p != null ? p : "";
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        int query = input.indexOf('?');
        if (query >= 0) {
            return input.substring(0, query);
        }
        int hash = input.indexOf('#');
        if (hash >= 0) {
            return input.substring(0, hash);
        }
        return input.startsWith("/") ? input : input;
    }

    private static List<String> splitDecodedSegments(String remainder) {
        if (remainder == null || remainder.isBlank()) {
            return List.of();
        }
        return Arrays.stream(remainder.split("/"))
                .map(PanopticumLinkResolverService::decodeSegment)
                .filter(s -> !s.isBlank())
                .toList();
    }

    private static String decodeSegment(String segment) {
        return URLDecoder.decode(segment, StandardCharsets.UTF_8);
    }

    public record ResolveOutcome(boolean success, String error, List<String> availablePaths, Map<String, Object> data) {

        static ResolveOutcome success(Map<String, Object> data) {
            return new ResolveOutcome(true, null, List.of(), data);
        }

        static ResolveOutcome error(String error, List<String> availablePaths) {
            return new ResolveOutcome(false, error, availablePaths, Map.of());
        }
    }
}
