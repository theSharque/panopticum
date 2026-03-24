package com.panopticum.elasticsearch.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.elasticsearch.model.ElasticsearchIndexInfo;
import com.panopticum.elasticsearch.model.ElasticsearchSearchResult;
import com.panopticum.elasticsearch.model.SearchResponseDto;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientException;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.BlockingHttpClient;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchClient {

    private final ObjectMapper objectMapper;

    @Client("/")
    private final HttpClient httpClient;

    public boolean probeTransport(String baseUrl, String username, String password) {
        return probeTransport(baseUrl, username, password, false);
    }

    public boolean probeTransport(String baseUrl, String username, String password, boolean logFailuresAtInfo) {
        String root = normalizeBaseUrl(baseUrl);
        String url = root + "/";
        if (isHttps(root)) {
            try {
                java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.get(url, username, password);
                int code = r.statusCode();

                return isProbeSuccessHttpStatus(code);
            } catch (Exception e) {
                logProbeTransportFailure(url, e.getMessage(), logFailuresAtInfo);

                return false;
            }
        }
        BlockingHttpClient client = httpClient.toBlocking();
        MutableHttpRequest<?> request = request(url, username, password);
        try {
            client.exchange(request, Argument.mapOf(String.class, Object.class));

            return true;
        } catch (HttpClientResponseException e) {
            int code = e.getStatus().getCode();
            if (isProbeSuccessHttpStatus(code)) {
                return true;
            }
            if (logFailuresAtInfo) {
                log.info("Elasticsearch probe: HTTP status {} for {} is not treated as reachable (e.g. nginx TLS mismatch)", code, url);
            }

            return false;
        } catch (HttpClientException e) {
            logProbeTransportFailure(url, e.getMessage(), logFailuresAtInfo);

            return false;
        } catch (Exception e) {
            logProbeTransportFailure(url, e.getMessage(), logFailuresAtInfo);

            return false;
        }
    }

    private static void logProbeTransportFailure(String url, String message, boolean logFailuresAtInfo) {
        if (logFailuresAtInfo) {
            log.info("Elasticsearch probe transport failed for {}: {}", url, message);
        } else {
            log.debug("Elasticsearch probe transport failed for {}: {}", url, message);
        }
    }

    private static boolean isProbeSuccessHttpStatus(int code) {
        if (code >= 200 && code < 300) {
            return true;
        }
        if (code == 401 || code == 403) {
            return true;
        }
        if (code >= 500) {
            return true;
        }

        return false;
    }

    public List<ElasticsearchIndexInfo> listIndices(String baseUrl, String username, String password) {
        String root = normalizeBaseUrl(baseUrl);
        if (isHttps(root)) {
            return listIndicesHttpsChain(root, username, password);
        }

        return listIndicesHttpChain(root, username, password);
    }

    private List<ElasticsearchIndexInfo> listIndicesHttpsChain(String root, String username, String password) {
        String urlFull = root + "/_cat/indices?format=json&h=index,docs.count,store.size";
        try {
            java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.get(urlFull, username, password);
            int code = r.statusCode();
            String body = r.body() != null ? r.body() : "";
            if (code == 200) {
                return objectMapper.readValue(body, new TypeReference<List<ElasticsearchIndexInfo>>() {
                });
            }
            if (code == 403 || code == 401) {
                log.debug("Elasticsearch listIndices cat(full) HTTP {} for {}, trying fallbacks", code, urlFull);

                return listIndicesHttpsFallbacks(root, username, password);
            }
            log.warn("Elasticsearch listIndices HTTP {} for {} body snippet: {}", code, urlFull,
                    truncateForLog(body));

            return Collections.emptyList();
        } catch (Exception e) {
            log.warn("Elasticsearch listIndices parse or request failed for {}: {}", urlFull, e.getMessage());

            return Collections.emptyList();
        }
    }

    private List<ElasticsearchIndexInfo> listIndicesHttpsFallbacks(String root, String username, String password) {
        String urlIndex = root + "/_cat/indices?format=json&h=index";
        try {
            java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.get(urlIndex, username, password);
            String body = r.body() != null ? r.body() : "";
            if (r.statusCode() == 200) {
                log.info("Elasticsearch listIndices: used cat h=index fallback");

                return objectMapper.readValue(body, new TypeReference<List<ElasticsearchIndexInfo>>() {
                });
            }
            log.debug("Elasticsearch listIndices cat(index-only) HTTP {} for {}", r.statusCode(), urlIndex);
        } catch (Exception e) {
            log.debug("Elasticsearch listIndices cat(index-only) failed: {}", e.getMessage());
        }

        String urlAliases = root + "/_aliases";
        try {
            java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.get(urlAliases, username, password);
            String body = r.body() != null ? r.body() : "";
            if (r.statusCode() == 200) {
                log.info("Elasticsearch listIndices: used _aliases fallback");

                return indicesFromAliasesJson(body);
            }
            log.warn("Elasticsearch listIndices _aliases HTTP {} for {} body snippet: {}", r.statusCode(), urlAliases,
                    truncateForLog(body));
        } catch (Exception e) {
            log.warn("Elasticsearch listIndices _aliases failed for {}: {}", urlAliases, e.getMessage());
        }

        return Collections.emptyList();
    }

    private List<ElasticsearchIndexInfo> listIndicesHttpChain(String root, String username, String password) {
        String urlFull = root + "/_cat/indices?format=json&h=index,docs.count,store.size";
        BlockingHttpClient client = httpClient.toBlocking();
        MutableHttpRequest<?> request = request(urlFull, username, password);
        try {
            HttpResponse<List<ElasticsearchIndexInfo>> response = client.exchange(
                    request,
                    Argument.listOf(ElasticsearchIndexInfo.class)
            );

            return response.getBody().orElse(Collections.emptyList());
        } catch (HttpClientResponseException e) {
            int code = e.getStatus().getCode();
            if (code == 403 || code == 401) {
                log.debug("Elasticsearch listIndices cat(full) HTTP {}, trying fallbacks", code);

                return listIndicesHttpFallbacks(root, username, password);
            }
            log.warn("Elasticsearch listIndices HTTP {} for {}: {}", code, urlFull, e.getMessage());

            return Collections.emptyList();
        } catch (HttpClientException e) {
            log.warn("Failed to connect to Elasticsearch {}: {}", urlFull, e.getMessage());

            return Collections.emptyList();
        }
    }

    private List<ElasticsearchIndexInfo> listIndicesHttpFallbacks(String root, String username, String password) {
        BlockingHttpClient client = httpClient.toBlocking();
        String urlIndex = root + "/_cat/indices?format=json&h=index";
        try {
            HttpResponse<List<ElasticsearchIndexInfo>> response = client.exchange(
                    request(urlIndex, username, password),
                    Argument.listOf(ElasticsearchIndexInfo.class)
            );
            log.info("Elasticsearch listIndices: used cat h=index fallback");

            return response.getBody().orElse(Collections.emptyList());
        } catch (HttpClientResponseException e) {
            log.debug("Elasticsearch listIndices cat(index-only) HTTP {} for {}", e.getStatus().getCode(), urlIndex);
        } catch (HttpClientException e) {
            log.debug("Elasticsearch listIndices cat(index-only) failed: {}", e.getMessage());
        }

        String urlAliases = root + "/_aliases";
        try {
            HttpResponse<Map<String, Object>> response = client.exchange(
                    request(urlAliases, username, password),
                    Argument.mapOf(String.class, Object.class)
            );
            Map<String, Object> map = response.getBody().orElse(Collections.emptyMap());
            log.info("Elasticsearch listIndices: used _aliases fallback");

            return indicesFromAliasesMap(map);
        } catch (HttpClientResponseException e) {
            log.warn("Elasticsearch listIndices _aliases HTTP {} for {}: {}", e.getStatus().getCode(), urlAliases,
                    e.getMessage());
        } catch (HttpClientException e) {
            log.warn("Elasticsearch listIndices _aliases failed for {}: {}", urlAliases, e.getMessage());
        }

        return Collections.emptyList();
    }

    private List<ElasticsearchIndexInfo> indicesFromAliasesJson(String body) throws Exception {
        Map<String, Object> m = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {
        });

        return indicesFromAliasesMap(m);
    }

    private List<ElasticsearchIndexInfo> indicesFromAliasesMap(Map<String, Object> m) {
        List<ElasticsearchIndexInfo> out = new ArrayList<>();
        for (String name : m.keySet()) {
            ElasticsearchIndexInfo info = new ElasticsearchIndexInfo();
            info.setIndex(name);
            out.add(info);
        }
        out.sort(Comparator.comparing(i -> i.getIndex() != null ? i.getIndex() : "", String.CASE_INSENSITIVE_ORDER));

        return out;
    }

    private static String truncateForLog(String body) {
        if (body == null) {
            return "";
        }
        String t = body.replace('\n', ' ').trim();
        if (t.length() > 400) {
            return t.substring(0, 400) + "…";
        }

        return t;
    }

    public Map<String, Object> getMapping(String baseUrl, String indexName, String username, String password) {
        String root = normalizeBaseUrl(baseUrl);
        String url = root + "/" + encodePath(indexName) + "/_mapping";
        if (isHttps(root)) {
            try {
                java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.get(url, username, password);
                if (r.statusCode() < 200 || r.statusCode() >= 300) {
                    log.debug("Elasticsearch getMapping failed for {}: status {}", url, r.statusCode());

                    return null;
                }

                return objectMapper.readValue(r.body(), new TypeReference<Map<String, Object>>() {
                });
            } catch (Exception e) {
                log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

                return null;
            }
        }
        BlockingHttpClient client = httpClient.toBlocking();
        MutableHttpRequest<?> request = request(url, username, password);
        try {
            HttpResponse<Map<String, Object>> response = client.exchange(request,
                    Argument.mapOf(String.class, Object.class));

            return response.getBody().orElse(Collections.emptyMap());
        } catch (HttpClientResponseException e) {
            log.debug("Elasticsearch getMapping failed for {}: {}", url, e.getMessage());

            return null;
        } catch (HttpClientException e) {
            log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

            return null;
        }
    }

    public ElasticsearchSearchResult search(String baseUrl, String indexName, String searchBody,
                                            String username, String password) {
        String root = normalizeBaseUrl(baseUrl);
        String url = root + "/" + encodePath(indexName) + "/_search";
        String body = searchBody != null && !searchBody.isBlank() ? searchBody : "{}";
        if (isHttps(root)) {
            try {
                java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.post(url, body, username, password);
                int code = r.statusCode();
                String respBody = r.body() != null ? r.body() : "";
                if (code < 200 || code >= 300) {
                    log.warn("Elasticsearch search HTTP {} for {} body snippet: {}", code, url, truncateForLog(respBody));

                    return ElasticsearchSearchResult.fail(
                            searchFailureMessage(code, respBody));
                }

                return ElasticsearchSearchResult.ok(objectMapper.readValue(respBody, SearchResponseDto.class));
            } catch (Exception e) {
                log.warn("Elasticsearch search failed for {}: {}", url, e.getMessage());

                return ElasticsearchSearchResult.fail(e.getMessage());
            }
        }
        BlockingHttpClient client = httpClient.toBlocking();
        MutableHttpRequest<String> request = HttpRequest.POST(url, body)
                .contentType(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON_TYPE);
        if (username != null && !username.isBlank()) {
            request.basicAuth(username, password != null ? password : "");
        }
        try {
            HttpResponse<SearchResponseDto> response = client.exchange(request, Argument.of(SearchResponseDto.class));
            SearchResponseDto dto = response.getBody().orElse(null);
            if (dto == null) {
                return ElasticsearchSearchResult.fail("error.queryExecutionFailed");
            }

            return ElasticsearchSearchResult.ok(dto);
        } catch (HttpClientResponseException e) {
            int code = e.getStatus().getCode();
            String errBody = httpErrorBodyAsString(e);
            log.warn("Elasticsearch search HTTP {} for {}: {}", code, url, truncateForLog(errBody));

            return ElasticsearchSearchResult.fail(searchFailureMessage(code, errBody));
        } catch (HttpClientException e) {
            log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

            return ElasticsearchSearchResult.fail(e.getMessage());
        }
    }

    private String httpErrorBodyAsString(HttpClientResponseException e) {
        try {
            return e.getResponse().getBody(String.class).orElse("");
        } catch (Exception ex) {
            return e.getMessage() != null ? e.getMessage() : "";
        }
    }

    private String searchFailureMessage(int httpStatus, String body) {
        String fromJson = extractOpenSearchErrorReason(body);
        if (fromJson != null && !fromJson.isBlank()) {
            return fromJson;
        }

        return "HTTP " + httpStatus;
    }

    private String extractOpenSearchErrorReason(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            JsonNode root = objectMapper.readTree(body);
            JsonNode err = root.get("error");
            if (err == null || err.isNull()) {
                return null;
            }
            if (err.isTextual()) {
                return err.asText();
            }
            JsonNode reason = err.get("reason");
            if (reason != null && reason.isTextual()) {
                return reason.asText();
            }
            JsonNode rc = err.get("root_cause");
            if (rc != null && rc.isArray() && rc.size() > 0) {
                JsonNode first = rc.get(0);
                JsonNode r = first.get("reason");
                if (r != null && r.isTextual()) {
                    return r.asText();
                }
            }
        } catch (Exception ignored) {
        }

        return null;
    }

    public Map<String, Object> getDocument(String baseUrl, String indexName, String docId,
                                           String username, String password) {
        String root = normalizeBaseUrl(baseUrl);
        String url = root + "/" + encodePath(indexName) + "/_doc/" + encodePath(docId);
        if (isHttps(root)) {
            try {
                java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.get(url, username, password);
                String b = r.body() != null ? r.body() : "";
                if (r.statusCode() < 200 || r.statusCode() >= 300) {
                    log.warn("Elasticsearch getDocument HTTP {} for {} body snippet: {}", r.statusCode(), url,
                            truncateForLog(b));

                    return null;
                }

                return objectMapper.readValue(b, new TypeReference<Map<String, Object>>() {
                });
            } catch (Exception e) {
                log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

                return null;
            }
        }
        BlockingHttpClient client = httpClient.toBlocking();
        MutableHttpRequest<?> request = request(url, username, password);
        try {
            HttpResponse<Map<String, Object>> response = client.exchange(request,
                    Argument.mapOf(String.class, Object.class));

            return response.getBody().orElse(null);
        } catch (HttpClientResponseException e) {
            log.warn("Elasticsearch getDocument HTTP {} for {}: {}", e.getStatus().getCode(), url, e.getMessage());

            return null;
        } catch (HttpClientException e) {
            log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

            return null;
        }
    }

    public boolean updateDocument(String baseUrl, String indexName, String docId, String sourceJson,
                                  String username, String password) {
        String root = normalizeBaseUrl(baseUrl);
        String url = root + "/" + encodePath(indexName) + "/_doc/" + encodePath(docId);
        if (isHttps(root)) {
            try {
                java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.put(url, sourceJson, username, password);

                return r.statusCode() >= 200 && r.statusCode() < 300;
            } catch (Exception e) {
                log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

                return false;
            }
        }
        BlockingHttpClient client = httpClient.toBlocking();
        MutableHttpRequest<String> request = HttpRequest.PUT(url, sourceJson)
                .contentType(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON_TYPE);
        if (username != null && !username.isBlank()) {
            request.basicAuth(username, password != null ? password : "");
        }
        try {
            client.exchange(request, Argument.mapOf(String.class, Object.class));

            return true;
        } catch (HttpClientResponseException e) {
            log.debug("Elasticsearch updateDocument failed for {}: {}", url, e.getMessage());

            return false;
        } catch (HttpClientException e) {
            log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

            return false;
        }
    }

    public boolean checkConnection(String baseUrl, String username, String password) {
        String root = normalizeBaseUrl(baseUrl);
        String url = root + "/";
        if (isHttps(root)) {
            try {
                java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.get(url, username, password);

                return r.statusCode() >= 200 && r.statusCode() < 300;
            } catch (Exception e) {
                log.info("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

                return false;
            }
        }
        BlockingHttpClient client = httpClient.toBlocking();
        MutableHttpRequest<?> request = request(url, username, password);
        try {
            client.exchange(request, Argument.mapOf(String.class, Object.class));

            return true;
        } catch (HttpClientResponseException e) {
            log.info("Elasticsearch checkConnection HTTP response error for {}: {}", url, e.getMessage());

            return false;
        } catch (HttpClientException e) {
            log.info("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

            return false;
        }
    }

    private static boolean isHttps(String root) {
        return root != null && root.regionMatches(true, 0, "https://", 0, 8);
    }

    private static String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            return "";
        }

        return baseUrl.trim();
    }

    private MutableHttpRequest<?> request(String url, String username, String password) {
        MutableHttpRequest<?> req = HttpRequest.GET(url)
                .accept(MediaType.APPLICATION_JSON_TYPE);
        if (username != null && !username.isBlank()) {
            req.basicAuth(username, password != null ? password : "");
        }

        return req;
    }

    private String encodePath(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }

        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
