package com.panopticum.elasticsearch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.StringUtils;
import com.panopticum.elasticsearch.client.ElasticsearchClient;
import com.panopticum.elasticsearch.model.ElasticsearchIndexInfo;
import com.panopticum.elasticsearch.model.SearchHitDto;
import com.panopticum.elasticsearch.model.SearchResponseDto;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
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

    public Optional<String> testConnection(String host, int port, String username, String password) {
        if (host == null || host.isBlank()) {
            return Optional.of("error.specifyHost");
        }
        String baseUrl = baseUrl(host, port);
        boolean ok = elasticsearchClient.checkConnection(baseUrl,
                username != null ? username : "",
                password != null ? password : "");
        return ok ? Optional.empty() : Optional.of("connectionTest.failed");
    }

    public List<ElasticsearchIndexInfo> listIndices(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .map(conn -> elasticsearchClient.listIndices(
                        baseUrl(conn.getHost(), conn.getPort()),
                        conn.getUsername() != null ? conn.getUsername() : "",
                        conn.getPassword() != null ? conn.getPassword() : ""))
                .orElse(List.of());
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

    private static java.util.Comparator<ElasticsearchIndexInfo> indexComparator(String sortBy, boolean desc) {
        java.util.Comparator<ElasticsearchIndexInfo> c = switch (sortBy) {
            case "docsCount" -> java.util.Comparator.comparingLong(ElasticsearchIndexInfo::getDocsCountNum);
            case "storeSize" -> java.util.Comparator.comparing(
                    i -> i.getStoreSize() != null ? i.getStoreSize() : "",
                    String.CASE_INSENSITIVE_ORDER);
            default -> java.util.Comparator.comparing(
                    i -> i.getIndex() != null ? i.getIndex() : "",
                    String.CASE_INSENSITIVE_ORDER);
        };
        return desc ? c.reversed() : c;
    }

    public Optional<Map<String, Object>> getMapping(Long connectionId, String indexName) {
        return dbConnectionService.findById(connectionId)
                .map(conn -> elasticsearchClient.getMapping(
                        baseUrl(conn.getHost(), conn.getPort()),
                        indexName,
                        conn.getUsername() != null ? conn.getUsername() : "",
                        conn.getPassword() != null ? conn.getPassword() : ""))
                .filter(m -> m != null && !m.isEmpty());
    }

    public Optional<QueryResult> search(Long connectionId, String indexName, String queryDsl, int offset, int limit) {
        if (indexName == null || indexName.isBlank()) {
            return Optional.of(QueryResult.error("error.specifyIndex"));
        }
        int lim = limit > 0 ? Math.min(limit, Math.min(queryRowsLimit, SEARCH_MAX_SIZE)) : 100;
        int off = Math.max(0, offset);
        String searchBody = buildSearchBody(queryDsl, off, lim);

        return dbConnectionService.findById(connectionId)
                .map(conn -> {
                    SearchResponseDto response = elasticsearchClient.search(
                            baseUrl(conn.getHost(), conn.getPort()),
                            indexName,
                            searchBody,
                            conn.getUsername() != null ? conn.getUsername() : "",
                            conn.getPassword() != null ? conn.getPassword() : "");
                    return searchResponseToQueryResult(response, off, lim);
                })
                .filter(r -> r != null);
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
        return dbConnectionService.findById(connectionId)
                .flatMap(conn -> {
                    Map<String, Object> doc = elasticsearchClient.getDocument(
                            baseUrl(conn.getHost(), conn.getPort()),
                            indexName, docId,
                            conn.getUsername() != null ? conn.getUsername() : "",
                            conn.getPassword() != null ? conn.getPassword() : "");
                    if (doc == null) {
                        return Optional.<String>empty();
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
                });
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
        boolean updated = dbConnectionService.findById(connectionId)
                .map(conn -> elasticsearchClient.updateDocument(
                        baseUrl(conn.getHost(), conn.getPort()),
                        indexName, docId, sourceJson,
                        conn.getUsername() != null ? conn.getUsername() : "",
                        conn.getPassword() != null ? conn.getPassword() : ""))
                .orElse(false);
        return updated ? Optional.empty() : Optional.of("error.queryExecutionFailed");
    }

    private static String baseUrl(String host, int port) {
        String h = host != null && !host.isBlank() ? host : "localhost";
        int p = port > 0 ? port : 9200;
        return "http://" + h + ":" + p;
    }
}
