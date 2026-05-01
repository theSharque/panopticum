package com.panopticum.elasticsearch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.core.error.ConnectionSupport;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.StringUtils;
import com.panopticum.elasticsearch.client.ElasticsearchClient;
import com.panopticum.elasticsearch.model.ElasticsearchIndexInfo;
import com.panopticum.elasticsearch.model.ElasticsearchSearchResult;
import com.panopticum.elasticsearch.model.SearchHitDto;
import com.panopticum.elasticsearch.model.SearchResponseDto;
import com.panopticum.mcp.model.ColumnInfo;
import com.panopticum.mcp.model.EntityDescription;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchService {

    private static final int SEARCH_MAX_SIZE = 1000;

    private final DbConnectionService dbConnectionService;
    private final ElasticsearchClient elasticsearchClient;
    private final ObjectMapper objectMapper;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    public Optional<String> testConnection(Optional<Long> connectionId, String host, Integer port,
                                          String username, String password) {
        if (host == null || host.isBlank()) {
            return Optional.of("error.specifyHost");
        }
        String user = username != null ? username : "";
        String pass = password != null ? password : "";
        int p = port != null && port > 0 ? port : 9200;
        String h = host.trim();
        DbConnection probe = elasticsearchProbeConnection(connectionId, h, p, user, pass);
        log.info("Elasticsearch connection test: host={} port={} connectionId={} useHttpsStored={} userSet={}",
                h, p, connectionId.orElse(null), probe.isUseHttps(), user != null && !user.isBlank());
        String baseUrl = resolveBaseUrl(probe, true);
        log.info("Elasticsearch connection test: resolved baseUrl={}", baseUrl);
        boolean ok = elasticsearchClient.checkConnection(baseUrl, user, pass);
        if (ok) {
            log.info("Elasticsearch connection test: success baseUrl={}", baseUrl);
        } else {
            log.info("Elasticsearch connection test: failed baseUrl={} (checkConnection returned false)", baseUrl);
        }

        return ok ? Optional.empty() : Optional.of("connectionTest.failed");
    }

    private DbConnection elasticsearchProbeConnection(Optional<Long> connectionId, String host, int port,
                                                     String user, String pass) {
        if (connectionId.isEmpty()) {
            return elasticsearchTransientProbe(host, port, user, pass);
        }
        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId.get());
        if (connOpt.isEmpty() || !"elasticsearch".equalsIgnoreCase(connOpt.get().getType())) {
            return elasticsearchTransientProbe(host, port, user, pass);
        }
        DbConnection stored = connOpt.get();

        return DbConnection.builder()
                .id(stored.getId())
                .name(stored.getName())
                .type(stored.getType())
                .host(host)
                .port(port)
                .dbName(stored.getDbName())
                .username(user)
                .password(pass)
                .useHttps(stored.isUseHttps())
                .createdAt(stored.getCreatedAt())
                .build();
    }

    private static DbConnection elasticsearchTransientProbe(String host, int port, String user, String pass) {
        return DbConnection.builder()
                .type("elasticsearch")
                .name("_")
                .host(host)
                .port(port)
                .dbName("")
                .username(user)
                .password(pass)
                .useHttps(false)
                .build();
    }

    public List<ElasticsearchIndexInfo> listIndices(Long connectionId) {
        DbConnection conn = requireElasticsearch(connectionId);
        return elasticsearchClient.listIndices(
                resolveBaseUrl(conn, false),
                conn.getUsername() != null ? conn.getUsername() : "",
                conn.getPassword() != null ? conn.getPassword() : "");
    }

    public Page<ElasticsearchIndexInfo> listIndicesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<ElasticsearchIndexInfo> all = listIndices(connectionId);
        String sortBy = sort != null ? sort : "index";
        boolean desc = "desc".equalsIgnoreCase(order);
        List<ElasticsearchIndexInfo> sorted = all.stream()
                .sorted(indexComparator(sortBy, desc))
                .toList();

        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    private static Comparator<ElasticsearchIndexInfo> indexComparator(String sortBy, boolean desc) {
        Comparator<ElasticsearchIndexInfo> c = switch (sortBy) {
            case "docsCount" -> Comparator.comparingLong(ElasticsearchIndexInfo::getDocsCountNum);
            case "storeSize" -> Comparator.comparing(
                    i -> i.getStoreSize() != null ? i.getStoreSize() : "",
                    String.CASE_INSENSITIVE_ORDER);
            default -> Comparator.comparing(
                    i -> i.getIndex() != null ? i.getIndex() : "",
                    String.CASE_INSENSITIVE_ORDER);
        };

        return desc ? c.reversed() : c;
    }

    public Optional<Map<String, Object>> getMapping(Long connectionId, String indexName) {
        DbConnection conn = requireElasticsearch(connectionId);
        Map<String, Object> m = elasticsearchClient.getMapping(
                resolveBaseUrl(conn, false),
                indexName,
                conn.getUsername() != null ? conn.getUsername() : "",
                conn.getPassword() != null ? conn.getPassword() : "");
        if (m == null || m.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(m);
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String indexName, String queryDsl, int offset, int limit) {
        if (indexName == null || indexName.isBlank()) {
            return Optional.of(QueryResult.error("error.specifyIndex"));
        }
        int lim = limit > 0 ? Math.min(limit, Math.min(queryRowsLimit, SEARCH_MAX_SIZE)) : 100;
        int off = Math.max(0, offset);
        String searchBody = buildSearchBody(queryDsl, off, lim);

        DbConnection conn = requireElasticsearch(connectionId);
        ElasticsearchSearchResult sr = elasticsearchClient.search(
                resolveBaseUrl(conn, false),
                indexName,
                searchBody,
                conn.getUsername() != null ? conn.getUsername() : "",
                conn.getPassword() != null ? conn.getPassword() : "");
        if (sr.failureMessage() != null) {
            return Optional.of(QueryResult.error(sr.failureMessage()));
        }
        if (sr.response() == null) {
            return Optional.of(QueryResult.error("error.queryExecutionFailed"));
        }

        return Optional.of(searchResponseToQueryResult(sr.response(), off, lim));
    }

    private QueryResult searchResponseToQueryResult(SearchResponseDto response, int offset, int limit) {
        if (response == null || response.getHits() == null) {
            return QueryResult.error("error.queryExecutionFailed");
        }
        List<SearchHitDto> hits = response.getHits().getHits();
        if (hits == null) {
            hits = List.of();
        }
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        columns.add("_id");
        for (SearchHitDto hit : hits) {
            if (hit.getSource() != null) {
                columns.addAll(hit.getSource().keySet());
            }
        }
        List<String> columnList = new ArrayList<>(columns);
        List<List<Object>> rows = new ArrayList<>();
        List<String> docIds = new ArrayList<>();
        for (SearchHitDto hit : hits) {
            List<Object> row = new ArrayList<>();
            Map<String, Object> source = hit.getSource() != null ? hit.getSource() : Map.of();
            for (String col : columnList) {
                if ("_id".equals(col)) {
                    row.add(StringUtils.truncateCell(hit.getId()));
                    docIds.add(hit.getId() != null ? hit.getId() : "");
                } else {
                    row.add(StringUtils.truncateCell(source.get(col)));
                }
            }
            rows.add(row);
        }
        boolean hasMore = hits.size() >= limit;

        return new QueryResult(columnList, null, rows, docIds, null, offset, limit, hasMore);
    }

    private String buildSearchBody(String queryDsl, int from, int size) {
        try {
            Map<String, Object> body = new TreeMap<>();
            if (queryDsl != null && !queryDsl.isBlank()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> parsed = objectMapper.readValue(queryDsl.trim(), Map.class);
                body.putAll(parsed);
            }
            if (!body.containsKey("query")) {
                body.put("query", Map.of("match_all", Map.of()));
            }
            body.put("from", from);
            body.put("size", size);

            return objectMapper.writeValueAsString(body);
        } catch (Exception e) {
            log.debug("Failed to parse query DSL, using match_all: {}", e.getMessage());

            return String.format("{\"query\":{\"match_all\":{}},\"from\":%d,\"size\":%d}", from, size);
        }
    }

    public Optional<String> getDocument(Long connectionId, String indexName, String docId) {
        DbConnection conn = requireElasticsearch(connectionId);
        Map<String, Object> doc = elasticsearchClient.getDocument(
                resolveBaseUrl(conn, false),
                indexName, docId,
                conn.getUsername() != null ? conn.getUsername() : "",
                conn.getPassword() != null ? conn.getPassword() : "");
        if (doc == null) {
            return Optional.empty();
        }
        Object source = doc.get("_source");
        if (source == null) {
            return Optional.of("{}");
        }
        try {
            return Optional.of(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(source));
        } catch (Exception e) {
            return Optional.of(source.toString());
        }
    }

    public Optional<String> updateDocument(Long connectionId, String indexName, String docId, String sourceJson) {
        if (docId == null || docId.isBlank() || indexName == null || indexName.isBlank()) {
            return Optional.of("error.specifyIndex");
        }
        if (sourceJson == null || sourceJson.isBlank()) {
            return Optional.of("error.invalidJson");
        }
        try {
            objectMapper.readTree(sourceJson);
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
        DbConnection conn = requireElasticsearch(connectionId);
        boolean updated = elasticsearchClient.updateDocument(
                resolveBaseUrl(conn, false),
                indexName, docId, sourceJson,
                conn.getUsername() != null ? conn.getUsername() : "",
                conn.getPassword() != null ? conn.getPassword() : "");

        return updated ? Optional.empty() : Optional.of("error.queryExecutionFailed");
    }

    private DbConnection requireElasticsearch(Long connectionId) {
        return ConnectionSupport.require(
                dbConnectionService.findById(connectionId).filter(c -> "elasticsearch".equalsIgnoreCase(c.getType())));
    }

    private String resolveBaseUrl(DbConnection conn, boolean logDetails) {
        String host = conn.getHost() != null && !conn.getHost().isBlank() ? conn.getHost().trim() : "localhost";
        int port = conn.getPort() > 0 ? conn.getPort() : 9200;
        String user = conn.getUsername() != null ? conn.getUsername() : "";
        String pass = conn.getPassword() != null ? conn.getPassword() : "";
        String httpUrl = "http://" + host + ":" + port;
        String httpsUrl = "https://" + host + ":" + port;

        if (logDetails) {
            log.info("Elasticsearch resolve: try host={} port={} useHttpsStored={} http={} https={}",
                    host, port, conn.isUseHttps(), httpUrl, httpsUrl);
        }

        if (conn.isUseHttps()) {
            boolean httpsOk = elasticsearchClient.probeTransport(httpsUrl, user, pass, logDetails);
            if (logDetails) {
                log.info("Elasticsearch resolve: stored useHttps=true probe https {} -> {}", httpsUrl, httpsOk);
            }
            if (httpsOk) {
                return httpsUrl;
            }
            boolean httpOk = elasticsearchClient.probeTransport(httpUrl, user, pass, logDetails);
            if (logDetails) {
                log.info("Elasticsearch resolve: probe http {} -> {}", httpUrl, httpOk);
            }
            if (httpOk) {
                persistUseHttps(conn, false);

                return httpUrl;
            }

            if (logDetails) {
                log.info("Elasticsearch resolve: both probes failed, fallback {}", httpsUrl);
            }

            return httpsUrl;
        }

        boolean httpFirst = elasticsearchClient.probeTransport(httpUrl, user, pass, logDetails);
        if (logDetails) {
            log.info("Elasticsearch resolve: stored useHttps=false probe http {} -> {}", httpUrl, httpFirst);
        }

        if (httpFirst) {
            return httpUrl;
        }

        boolean httpsSecond = elasticsearchClient.probeTransport(httpsUrl, user, pass, logDetails);
        if (logDetails) {
            log.info("Elasticsearch resolve: probe https {} -> {}", httpsUrl, httpsSecond);
        }

        if (httpsSecond) {
            persistUseHttps(conn, true);

            return httpsUrl;
        }

        if (logDetails) {
            log.info("Elasticsearch resolve: both probes failed, fallback {}", httpUrl);
        }

        return httpUrl;
    }

    private void persistUseHttps(DbConnection conn, boolean useHttps) {
        if (conn.getId() == null) {
            return;
        }

        if (conn.isUseHttps() == useHttps) {
            return;
        }

        conn.setUseHttps(useHttps);
        dbConnectionService.save(conn);
        log.info("Elasticsearch: saved use_https={} for connection id={}", useHttps, conn.getId());
    }

    public Optional<EntityDescription> describeIndex(Long connectionId, String indexName) {
        try {
            DbConnection conn = requireElasticsearch(connectionId);
            String baseUrl = resolveBaseUrl(conn, false);
            String user = conn.getUsername() != null ? conn.getUsername() : "";
            String pass = conn.getPassword() != null ? conn.getPassword() : "";
            Map<String, Object> mapping = elasticsearchClient.getMapping(baseUrl, indexName, user, pass);
            List<ColumnInfo> columns = flattenEsMapping(mapping, indexName);

            return Optional.of(EntityDescription.builder()
                    .entityKind("index")
                    .catalog(indexName)
                    .namespace(null)
                    .entity(indexName)
                    .columns(columns)
                    .primaryKey(List.of("_id"))
                    .foreignKeys(List.of())
                    .indexes(List.of())
                    .approximateRowCount(null)
                    .inferredFromSample(false)
                    .notes(List.of())
                    .build());
        } catch (Exception e) {
            log.warn("describeIndex failed for {}: {}", indexName, e.getMessage());
            return Optional.empty();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<ColumnInfo> flattenEsMapping(Map<String, Object> rawMapping, String indexName) {
        List<ColumnInfo> result = new ArrayList<>();
        try {
            Object indexEntry = rawMapping.get(indexName);
            if (!(indexEntry instanceof Map)) return result;

            Object mappings = ((Map<String, Object>) indexEntry).get("mappings");
            if (!(mappings instanceof Map)) return result;

            Object props = ((Map<String, Object>) mappings).get("properties");
            if (!(props instanceof Map)) return result;

            int pos = 1;
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) props).entrySet()) {
                String type = "object";
                if (entry.getValue() instanceof Map) {
                    Object t = ((Map<String, Object>) entry.getValue()).get("type");
                    if (t != null) type = t.toString();
                }

                result.add(ColumnInfo.builder()
                        .name(entry.getKey())
                        .type(type)
                        .nullable(true)
                        .primaryKey(false)
                        .position(pos++)
                        .build());
            }
        } catch (Exception e) {
            log.debug("flattenEsMapping failed: {}", e.getMessage());
        }

        return result;
    }
}
