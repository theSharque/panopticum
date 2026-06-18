package com.panopticum.core.audit;

import com.panopticum.core.model.ConnectionType;
import lombok.Value;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public final class AuditBrowseMatcher {

    private static final Set<String> SKIP_SUFFIXES = Set.of(
            "/sql", "/cql", "/detail", "/query", "/row/detail", "/document", "/documents", "/messages", "/logs", "/value");

    private AuditBrowseMatcher() {
    }

    @Value
    public static class BrowseMatch {
        Long connectionId;
        String connectionType;
        AuditAction action;
        String scope;
    }

    public static Optional<BrowseMatch> match(String method, String path) {
        if (method == null || !"GET".equalsIgnoreCase(method) || path == null || path.isBlank()) {
            return Optional.empty();
        }

        String normalized = stripQuery(path);
        if (isExcludedPath(normalized) || hasSkipSuffix(normalized)) {
            return Optional.empty();
        }

        for (ConnectionType type : ConnectionType.values()) {
            Optional<BrowseMatch> ui = matchUi(type, normalized);
            if (ui.isPresent()) {
                return ui;
            }

            if (type.getApiPathPrefix() != null) {
                Optional<BrowseMatch> api = matchApi(type, normalized);
                if (api.isPresent()) {
                    return api;
                }
            }
        }

        return matchInfraUi(normalized);
    }

    private static Optional<BrowseMatch> matchUi(ConnectionType type, String path) {
        String prefix = type.getUiPathPrefix();
        if (!path.startsWith(prefix + "/")) {
            return Optional.empty();
        }

        String remainder = path.substring(prefix.length() + 1);
        List<String> segments = splitSegments(remainder);
        if (segments.isEmpty() || !isConnectionId(segments.get(0))) {
            return Optional.empty();
        }

        Long connId = Long.parseLong(segments.get(0));
        List<String> scopeSegments = segments.subList(1, segments.size());
        if (scopeSegments.isEmpty()) {
            return Optional.empty();
        }

        return mapHierarchy(type, connId, scopeSegments);
    }

    private static Optional<BrowseMatch> matchApi(ConnectionType type, String path) {
        String prefix = type.getApiPathPrefix();
        if (prefix == null || !path.startsWith(prefix + "/")) {
            return Optional.empty();
        }

        String remainder = path.substring(prefix.length() + 1);
        List<String> segments = splitSegments(remainder);
        if (segments.isEmpty() || !isConnectionId(segments.get(0))) {
            return Optional.empty();
        }

        Long connId = Long.parseLong(segments.get(0));
        return matchApiSegments(type, connId, segments.subList(1, segments.size()));
    }

    private static Optional<BrowseMatch> matchApiSegments(ConnectionType type, Long connId, List<String> segments) {
        if (segments.isEmpty()) {
            return Optional.empty();
        }

        String canonical = type.getCanonicalId();
        String head = segments.get(0).toLowerCase(Locale.ROOT);

        return switch (head) {
            case "databases" -> matchDatabasesApi(type, connId, canonical, segments);
            case "schemas" -> matchSchemasApi(type, connId, canonical, segments);
            case "keyspaces" -> matchKeyspacesApi(connId, canonical, segments);
            case "buckets" -> matchBucketsApi(connId, canonical, segments);
            case "catalogs" -> optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, "");
            case "indices" -> matchIndicesApi(connId, canonical, segments);
            case "topics" -> matchTopicsApi(connId, canonical, segments);
            case "queues" -> matchQueuesApi(connId, canonical, segments);
            default -> Optional.empty();
        };
    }

    private static Optional<BrowseMatch> matchDatabasesApi(ConnectionType type, Long connId, String canonical,
                                                           List<String> segments) {
        if (segments.size() == 1) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, "");
        }

        if (segments.size() == 3 && "schemas".equalsIgnoreCase(segments.get(2))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_SCHEMA, decode(segments.get(1)));
        }

        if (segments.size() == 5 && "schemas".equalsIgnoreCase(segments.get(2))
                && "tables".equalsIgnoreCase(segments.get(4))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE,
                    decode(segments.get(1)) + "/" + decode(segments.get(3)));
        }

        if (segments.size() == 3 && "tables".equalsIgnoreCase(segments.get(2))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, decode(segments.get(1)));
        }

        if (segments.size() == 3 && "collections".equalsIgnoreCase(segments.get(2))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, decode(segments.get(1)));
        }

        if (segments.size() == 3 && "keys".equalsIgnoreCase(segments.get(2))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, decode(segments.get(1)));
        }

        if (segments.size() >= 2) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, decode(segments.get(1)));
        }

        return Optional.empty();
    }

    private static Optional<BrowseMatch> matchSchemasApi(ConnectionType type, Long connId, String canonical,
                                                         List<String> segments) {
        if (segments.size() == 1) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_SCHEMA, "");
        }

        if (segments.size() == 3 && "tables".equalsIgnoreCase(segments.get(2))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, decode(segments.get(1)));
        }

        if (segments.size() >= 2) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_SCHEMA, decode(segments.get(1)));
        }

        return Optional.empty();
    }

    private static Optional<BrowseMatch> matchKeyspacesApi(Long connId, String canonical, List<String> segments) {
        if (segments.size() == 1) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, "");
        }

        if (segments.size() == 3 && "tables".equalsIgnoreCase(segments.get(2))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, decode(segments.get(1)));
        }

        return Optional.empty();
    }

    private static Optional<BrowseMatch> matchBucketsApi(Long connId, String canonical, List<String> segments) {
        if (segments.size() == 1) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, "");
        }

        if (segments.size() == 3 && ("scopes".equalsIgnoreCase(segments.get(2))
                || "schemas".equalsIgnoreCase(segments.get(2)))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_SCHEMA, decode(segments.get(1)));
        }

        if (segments.size() == 5 && "scopes".equalsIgnoreCase(segments.get(2))
                && "tables".equalsIgnoreCase(segments.get(4))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE,
                    decode(segments.get(1)) + "/" + decode(segments.get(3)));
        }

        return optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, decode(segments.get(1)));
    }

    private static Optional<BrowseMatch> matchIndicesApi(Long connId, String canonical, List<String> segments) {
        if (segments.size() == 1) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, "");
        }

        return Optional.empty();
    }

    private static Optional<BrowseMatch> matchTopicsApi(Long connId, String canonical, List<String> segments) {
        if (segments.size() == 1) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, "");
        }

        if (segments.size() == 3 && "partitions".equalsIgnoreCase(segments.get(2))) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_SCHEMA, decode(segments.get(1)));
        }

        return optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, decode(segments.get(1)));
    }

    private static Optional<BrowseMatch> matchQueuesApi(Long connId, String canonical, List<String> segments) {
        if (segments.size() == 1) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, "");
        }

        if (segments.size() >= 3) {
            return optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE,
                    decode(segments.get(1)) + "/" + decode(segments.get(2)));
        }

        return Optional.empty();
    }

    private static Optional<BrowseMatch> mapHierarchy(ConnectionType type, Long connId, List<String> scopeSegments) {
        String hierarchy = type.getHierarchyModel();
        String canonical = type.getCanonicalId();
        int depth = scopeSegments.size();

        return switch (hierarchy) {
            case "catalog.namespace.entity" -> switch (depth) {
                case 1 -> optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, decode(scopeSegments.get(0)));
                case 2 -> optionalBrowse(connId, canonical, AuditAction.OPEN_SCHEMA, joinDecoded(scopeSegments, 2));
                case 3 -> optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, joinDecoded(scopeSegments, 3));
                default -> depth > 3
                        ? optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, joinDecoded(scopeSegments, 3))
                        : Optional.empty();
            };
            case "catalog.entity" -> switch (depth) {
                case 1 -> optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE, decode(scopeSegments.get(0)));
                case 2 -> optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, joinDecoded(scopeSegments, 2));
                default -> depth > 2
                        ? optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, joinDecoded(scopeSegments, 2))
                        : Optional.empty();
            };
            case "namespace.entity" -> switch (depth) {
                case 1 -> optionalBrowse(connId, canonical, AuditAction.OPEN_SCHEMA, decode(scopeSegments.get(0)));
                case 2 -> optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, joinDecoded(scopeSegments, 2));
                default -> depth > 2
                        ? optionalBrowse(connId, canonical, AuditAction.OPEN_TABLE, joinDecoded(scopeSegments, 2))
                        : Optional.empty();
            };
            case "catalog" -> optionalBrowse(connId, canonical, AuditAction.OPEN_DATABASE,
                    joinDecoded(scopeSegments, Math.min(depth, 1)));
            default -> matchDepthFallback(connId, canonical, scopeSegments);
        };
    }

    private static Optional<BrowseMatch> matchInfraUi(String path) {
        for (String prefix : List.of("/kubernetes", "/s3", "/prometheus", "/kafka", "/rabbitmq")) {
            if (!path.startsWith(prefix + "/")) {
                continue;
            }

            String remainder = path.substring(prefix.length() + 1);
            List<String> segments = splitSegments(remainder);
            if (segments.isEmpty() || !isConnectionId(segments.get(0))) {
                return Optional.empty();
            }

            Long connId = Long.parseLong(segments.get(0));
            String type = prefix.substring(1);
            List<String> scopeSegments = segments.subList(1, segments.size());
            if (scopeSegments.isEmpty()) {
                return Optional.empty();
            }

            return matchDepthFallback(connId, type, scopeSegments);
        }

        return Optional.empty();
    }

    private static Optional<BrowseMatch> matchDepthFallback(Long connId, String type, List<String> scopeSegments) {
        int depth = scopeSegments.size();
        if (depth == 1) {
            return optionalBrowse(connId, type, AuditAction.OPEN_DATABASE, decode(scopeSegments.get(0)));
        }
        if (depth == 2) {
            return optionalBrowse(connId, type, AuditAction.OPEN_SCHEMA, joinDecoded(scopeSegments, 2));
        }

        return optionalBrowse(connId, type, AuditAction.OPEN_TABLE, joinDecoded(scopeSegments, Math.min(depth, 3)));
    }

    private static Optional<BrowseMatch> optionalBrowse(Long connId, String type, AuditAction action, String scope) {
        return Optional.of(new BrowseMatch(connId, type, action, scope));
    }

    private static String joinDecoded(List<String> segments, int count) {
        String[] parts = new String[count];
        for (int i = 0; i < count; i++) {
            parts[i] = decode(segments.get(i));
        }

        return String.join("/", parts);
    }

    private static String decode(String segment) {
        return URLDecoder.decode(segment, StandardCharsets.UTF_8);
    }

    private static List<String> splitSegments(String path) {
        return Arrays.stream(path.split("/"))
                .filter(s -> !s.isBlank())
                .toList();
    }

    private static boolean isConnectionId(String segment) {
        try {
            Long.parseLong(segment);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static String stripQuery(String path) {
        int query = path.indexOf('?');
        return query >= 0 ? path.substring(0, query) : path;
    }

    private static boolean isExcludedPath(String path) {
        return path.equals("/")
                || path.startsWith("/login")
                || path.startsWith("/actuator")
                || path.startsWith("/swagger")
                || path.startsWith("/css/")
                || path.startsWith("/js/")
                || path.startsWith("/vendor/")
                || path.startsWith("/favicon")
                || path.startsWith("/theme")
                || path.startsWith("/locale")
                || path.startsWith("/settings")
                || path.startsWith("/mcp")
                || path.startsWith("/api/connections");
    }

    private static boolean hasSkipSuffix(String path) {
        String lower = path.toLowerCase(Locale.ROOT);
        for (String suffix : SKIP_SUFFIXES) {
            if (lower.endsWith(suffix)) {
                return true;
            }
        }

        return false;
    }
}
