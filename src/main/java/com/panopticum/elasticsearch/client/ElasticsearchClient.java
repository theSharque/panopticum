package com.panopticum.elasticsearch.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.elasticsearch.model.ElasticsearchIndexInfo;
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
import java.util.Collections;
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
        String url = root + "/_cat/indices?format=json&h=index,docs.count,store.size";
        if (isHttps(root)) {
            try {
                java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.get(url, username, password);
                if (r.statusCode() < 200 || r.statusCode() >= 300) {
                    log.debug("Elasticsearch listIndices failed for {}: status {}", url, r.statusCode());

                    return Collections.emptyList();
                }

                return objectMapper.readValue(r.body(), new TypeReference<List<ElasticsearchIndexInfo>>() {
                });
            } catch (Exception e) {
                log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

                return Collections.emptyList();
            }
        }
        BlockingHttpClient client = httpClient.toBlocking();
        MutableHttpRequest<?> request = request(url, username, password);
        try {
            HttpResponse<List<ElasticsearchIndexInfo>> response = client.exchange(
                    request,
                    Argument.listOf(ElasticsearchIndexInfo.class)
            );

            return response.getBody().orElse(Collections.emptyList());
        } catch (HttpClientResponseException e) {
            log.debug("Elasticsearch listIndices failed for {}: {}", url, e.getMessage());

            return Collections.emptyList();
        } catch (HttpClientException e) {
            log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

            return Collections.emptyList();
        }
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

    public SearchResponseDto search(String baseUrl, String indexName, String searchBody,
                                    String username, String password) {
        String root = normalizeBaseUrl(baseUrl);
        String url = root + "/" + encodePath(indexName) + "/_search";
        String body = searchBody != null && !searchBody.isBlank() ? searchBody : "{}";
        if (isHttps(root)) {
            try {
                java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.post(url, body, username, password);
                if (r.statusCode() < 200 || r.statusCode() >= 300) {
                    log.debug("Elasticsearch search failed for {}: status {}", url, r.statusCode());

                    return null;
                }

                return objectMapper.readValue(r.body(), SearchResponseDto.class);
            } catch (Exception e) {
                log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

                return null;
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

            return response.getBody().orElse(null);
        } catch (HttpClientResponseException e) {
            log.debug("Elasticsearch search failed for {}: {}", url, e.getMessage());

            return null;
        } catch (HttpClientException e) {
            log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());

            return null;
        }
    }

    public Map<String, Object> getDocument(String baseUrl, String indexName, String docId,
                                           String username, String password) {
        String root = normalizeBaseUrl(baseUrl);
        String url = root + "/" + encodePath(indexName) + "/_doc/" + encodePath(docId);
        if (isHttps(root)) {
            try {
                java.net.http.HttpResponse<String> r = ElasticsearchJdkHttps.get(url, username, password);
                if (r.statusCode() < 200 || r.statusCode() >= 300) {
                    log.debug("Elasticsearch getDocument failed for {}: status {}", url, r.statusCode());

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

            return response.getBody().orElse(null);
        } catch (HttpClientResponseException e) {
            log.debug("Elasticsearch getDocument failed for {}: {}", url, e.getMessage());

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
