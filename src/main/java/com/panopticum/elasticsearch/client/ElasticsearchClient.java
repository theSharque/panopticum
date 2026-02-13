package com.panopticum.elasticsearch.client;

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

    @Client("/")
    private final HttpClient httpClient;

    public List<ElasticsearchIndexInfo> listIndices(String baseUrl, String username, String password) {
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/_cat/indices?format=json&h=index,docs.count,store.size";
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
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/" + encodePath(indexName) + "/_mapping";
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
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/" + encodePath(indexName) + "/_search";
        String body = searchBody != null && !searchBody.isBlank() ? searchBody : "{}";
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
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/" + encodePath(indexName) + "/_doc/" + encodePath(docId);
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
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/" + encodePath(indexName) + "/_doc/" + encodePath(docId);
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
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/";
        MutableHttpRequest<?> request = request(url, username, password);
        try {
            client.exchange(request, Argument.mapOf(String.class, Object.class));
            return true;
        } catch (HttpClientResponseException e) {
            log.debug("Elasticsearch checkConnection failed for {}: {}", url, e.getMessage());
            return false;
        } catch (HttpClientException e) {
            log.warn("Failed to connect to Elasticsearch {}: {}", url, e.getMessage());
            return false;
        }
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
