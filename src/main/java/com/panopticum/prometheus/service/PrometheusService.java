package com.panopticum.prometheus.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.core.error.AccessResult;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.mcp.model.ColumnInfo;
import com.panopticum.mcp.model.EntityDescription;
import com.panopticum.prometheus.model.PromMetricInfo;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class PrometheusService {

    private static final int METRICS_LIMIT = 500;
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    private final DbConnectionService dbConnectionService;
    private final ObjectMapper objectMapper;

    public Optional<String> testConnection(String host, int port, String username, String password, boolean useHttps) {
        String baseUrl = buildBaseUrl(host, port, useHttps);
        try {
            HttpResponse<String> response = get(baseUrl + "/-/healthy", username, password);
            if (response.statusCode() == 200) {
                return Optional.empty();
            }
            if (response.statusCode() == 401) {
                return Optional.of("prometheus.access.unauthorized");
            }
            HttpResponse<String> fallback = get(baseUrl + "/api/v1/status/buildinfo", username, password);
            if (fallback.statusCode() == 200) {
                return Optional.empty();
            }
            if (fallback.statusCode() == 401) {
                return Optional.of("prometheus.access.unauthorized");
            }
            return Optional.of("prometheus.access.error");
        } catch (Exception e) {
            log.warn("Prometheus test connection failed: {}", e.getMessage());
            return Optional.of("prometheus.access.error");
        }
    }

    public AccessResult<List<String>> listJobs(Long connectionId) {
        return withConnection(connectionId, (conn, baseUrl) -> {
            HttpResponse<String> response = get(baseUrl + "/api/v1/label/job/values", conn.getUsername(), conn.getPassword());
            if (response.statusCode() == 401) {
                return AccessResult.unauthorized("prometheus.access.unauthorized");
            }
            if (response.statusCode() != 200) {
                return AccessResult.error("prometheus.access.error");
            }
            JsonNode root = objectMapper.readTree(response.body());
            List<String> jobs = new ArrayList<>();
            JsonNode data = root.path("data");
            if (data.isArray()) {
                for (JsonNode item : data) {
                    jobs.add(item.asText());
                }
            }
            return AccessResult.ok(jobs);
        });
    }

    public AccessResult<Page<PromMetricInfo>> listMetrics(Long connectionId, String job, int page, int size) {
        return withConnection(connectionId, (conn, baseUrl) -> {
            String url = buildMetricsUrl(baseUrl, job);
            HttpResponse<String> response = get(url, conn.getUsername(), conn.getPassword());
            if (response.statusCode() == 401) {
                return AccessResult.unauthorized("prometheus.access.unauthorized");
            }
            if (response.statusCode() != 200) {
                return AccessResult.error("prometheus.access.error");
            }
            JsonNode root = objectMapper.readTree(response.body());
            Set<String> metricNames = new LinkedHashSet<>();
            JsonNode data = root.path("data");
            if (data.isArray()) {
                for (JsonNode series : data) {
                    JsonNode name = series.path("__name__");
                    if (!name.isMissingNode()) {
                        metricNames.add(name.asText());
                    }
                }
            }
            List<String> sorted = new ArrayList<>(metricNames);
            sorted.sort(String::compareTo);

            int offset = (page - 1) * size;
            int end = Math.min(offset + size, sorted.size());
            List<PromMetricInfo> items = new ArrayList<>();
            for (int i = offset; i < end && i < sorted.size(); i++) {
                items.add(PromMetricInfo.builder().name(sorted.get(i)).job(job).build());
            }
            return AccessResult.ok(Page.of(items, page, size, "name", "asc"));
        });
    }

    public AccessResult<QueryResult> executeInstant(Long connectionId, String promql) {
        return withConnection(connectionId, (conn, baseUrl) -> {
            String encoded = URLEncoder.encode(promql, StandardCharsets.UTF_8);
            String url = baseUrl + "/api/v1/query?query=" + encoded;
            HttpResponse<String> response = get(url, conn.getUsername(), conn.getPassword());
            if (response.statusCode() == 401) {
                return AccessResult.unauthorized("prometheus.access.unauthorized");
            }
            if (response.statusCode() != 200) {
                return AccessResult.error("prometheus.access.error");
            }
            return AccessResult.ok(parseInstantResult(response.body()));
        });
    }

    public AccessResult<QueryResult> executeRange(Long connectionId, String promql, String start, String end, String step) {
        return withConnection(connectionId, (conn, baseUrl) -> {
            String encoded = URLEncoder.encode(promql, StandardCharsets.UTF_8);
            String safeStart = start != null && !start.isBlank() ? URLEncoder.encode(start, StandardCharsets.UTF_8) : "now-1h";
            String safeEnd = end != null && !end.isBlank() ? URLEncoder.encode(end, StandardCharsets.UTF_8) : "now";
            String safeStep = step != null && !step.isBlank() ? URLEncoder.encode(step, StandardCharsets.UTF_8) : "60";
            String url = baseUrl + "/api/v1/query_range?query=" + encoded
                    + "&start=" + safeStart + "&end=" + safeEnd + "&step=" + safeStep;
            HttpResponse<String> response = get(url, conn.getUsername(), conn.getPassword());
            if (response.statusCode() == 401) {
                return AccessResult.unauthorized("prometheus.access.unauthorized");
            }
            if (response.statusCode() != 200) {
                return AccessResult.error("prometheus.access.error");
            }
            return AccessResult.ok(parseRangeResult(response.body()));
        });
    }

    public AccessResult<EntityDescription> describeMetric(Long connectionId, String metric) {
        return withConnection(connectionId, (conn, baseUrl) -> {
            String encoded = URLEncoder.encode(metric, StandardCharsets.UTF_8);
            String url = baseUrl + "/api/v1/labels?match[]=" + encoded;
            HttpResponse<String> response = get(url, conn.getUsername(), conn.getPassword());
            if (response.statusCode() == 401) {
                return AccessResult.unauthorized("prometheus.access.unauthorized");
            }
            if (response.statusCode() != 200) {
                return AccessResult.error("prometheus.access.error");
            }
            JsonNode root = objectMapper.readTree(response.body());
            List<ColumnInfo> columns = new ArrayList<>();
            columns.add(ColumnInfo.builder().name("__name__").type("string").nullable(false).primaryKey(true).position(1).build());
            columns.add(ColumnInfo.builder().name("timestamp").type("float64").nullable(false).primaryKey(false).position(2).build());
            columns.add(ColumnInfo.builder().name("value").type("string").nullable(false).primaryKey(false).position(3).build());
            int pos = 4;
            JsonNode data = root.path("data");
            if (data.isArray()) {
                for (JsonNode label : data) {
                    String labelName = label.asText();
                    if (!"__name__".equals(labelName)) {
                        columns.add(ColumnInfo.builder().name(labelName).type("string").nullable(true).primaryKey(false).position(pos++).build());
                    }
                }
            }
            return AccessResult.ok(EntityDescription.builder()
                    .entityKind("metric")
                    .catalog(null)
                    .namespace(null)
                    .entity(metric)
                    .columns(columns)
                    .primaryKey(List.of("__name__", "timestamp"))
                    .foreignKeys(List.of())
                    .indexes(List.of())
                    .approximateRowCount(null)
                    .inferredFromSample(false)
                    .notes(List.of("type=gauge/counter/histogram (use /api/v1/metadata for details)"))
                    .build());
        });
    }

    private QueryResult parseInstantResult(String body) throws Exception {
        JsonNode root = objectMapper.readTree(body);
        JsonNode results = root.path("data").path("result");
        if (!results.isArray() || results.isEmpty()) {
            return QueryResult.error(null);
        }
        Set<String> labelKeys = collectLabelKeys(results);
        List<String> columns = buildColumns(labelKeys);
        List<List<Object>> rows = new ArrayList<>();
        for (JsonNode series : results) {
            JsonNode value = series.path("value");
            if (value.isArray() && value.size() == 2) {
                rows.add(buildRow(series.path("metric"), value.get(0).asText(), value.get(1).asText(), labelKeys));
            }
        }
        return new QueryResult(columns, rows, null, 0, rows.size(), false);
    }

    private QueryResult parseRangeResult(String body) throws Exception {
        JsonNode root = objectMapper.readTree(body);
        JsonNode results = root.path("data").path("result");
        if (!results.isArray() || results.isEmpty()) {
            return QueryResult.error(null);
        }
        Set<String> labelKeys = collectLabelKeys(results);
        List<String> columns = buildColumns(labelKeys);
        List<List<Object>> rows = new ArrayList<>();
        for (JsonNode series : results) {
            JsonNode values = series.path("values");
            if (values.isArray()) {
                for (JsonNode point : values) {
                    if (point.isArray() && point.size() == 2) {
                        rows.add(buildRow(series.path("metric"), point.get(0).asText(), point.get(1).asText(), labelKeys));
                    }
                }
            }
        }
        return new QueryResult(columns, rows, null, 0, rows.size(), false);
    }

    private Set<String> collectLabelKeys(JsonNode results) {
        Set<String> keys = new LinkedHashSet<>();
        for (JsonNode series : results) {
            JsonNode metric = series.path("metric");
            metric.fieldNames().forEachRemaining(k -> {
                if (!"__name__".equals(k)) {
                    keys.add(k);
                }
            });
        }
        return keys;
    }

    private List<String> buildColumns(Set<String> labelKeys) {
        List<String> cols = new ArrayList<>();
        cols.add("timestamp");
        cols.add("value");
        cols.addAll(labelKeys);
        return cols;
    }

    private List<Object> buildRow(JsonNode metric, String ts, String value, Set<String> labelKeys) {
        List<Object> row = new ArrayList<>();
        row.add(ts);
        row.add(value);
        for (String key : labelKeys) {
            JsonNode label = metric.path(key);
            row.add(label.isMissingNode() ? null : label.asText());
        }
        return row;
    }

    private String buildMetricsUrl(String baseUrl, String job) {
        if (job != null && !job.isBlank()) {
            String encodedJob = URLEncoder.encode(job, StandardCharsets.UTF_8);
            return baseUrl + "/api/v1/series?match[]=%7Bjob%3D%22" + encodedJob + "%22%7D";
        }
        return baseUrl + "/api/v1/series?match[]=%7B__name__%3D~%22.%2B%22%7D&limit=" + METRICS_LIMIT;
    }

    private <T> AccessResult<T> withConnection(Long connectionId, PrometheusAction<T> action) {
        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId);
        if (connOpt.isEmpty()) {
            return AccessResult.notFound("connection.notFound");
        }
        DbConnection conn = connOpt.get();
        String baseUrl = buildBaseUrl(conn.getHost(), conn.getPort(), conn.isUseHttps());
        try {
            return action.execute(conn, baseUrl);
        } catch (Exception e) {
            log.warn("Prometheus action failed for connection {}: {}", connectionId, e.getMessage());
            return AccessResult.error("prometheus.access.error");
        }
    }

    private HttpResponse<String> get(String url, String username, String password) throws Exception {
        HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(TIMEOUT)
                .GET();
        if (username != null && !username.isBlank()) {
            String encoded = Base64.getEncoder().encodeToString((username + ":" + (password != null ? password : "")).getBytes(StandardCharsets.UTF_8));
            builder.header("Authorization", "Basic " + encoded);
        } else if (password != null && !password.isBlank()) {
            builder.header("Authorization", "Bearer " + password);
        }
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private String buildBaseUrl(String host, int port, boolean useHttps) {
        if (host.startsWith("http://") || host.startsWith("https://")) {
            return host;
        }
        String scheme = useHttps ? "https" : "http";
        boolean standardPort = useHttps ? port == 443 : port == 80;
        return standardPort || port <= 0 ? scheme + "://" + host : scheme + "://" + host + ":" + port;
    }

    @FunctionalInterface
    private interface PrometheusAction<T> {
        AccessResult<T> execute(DbConnection conn, String baseUrl) throws Exception;
    }
}
