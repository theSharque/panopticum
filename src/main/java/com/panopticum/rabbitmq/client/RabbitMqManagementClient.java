package com.panopticum.rabbitmq.client;

import com.panopticum.rabbitmq.model.RabbitMqGetMessagesRequest;
import com.panopticum.rabbitmq.model.RabbitMqMessage;
import com.panopticum.rabbitmq.model.RabbitMqQueueInfo;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientException;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
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
public class RabbitMqManagementClient {

    @Client("/")
    private final HttpClient httpClient;

    public List<RabbitMqQueueInfo> listQueues(String baseUrl, String username, String password) {
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/api/queues";
        MutableHttpRequest<?> request = HttpRequest.GET(url)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .basicAuth(username, password);
        try {
            HttpResponse<List<RabbitMqQueueInfo>> response = client.exchange(
                    request,
                    Argument.listOf(RabbitMqQueueInfo.class)
            );
            return response.getBody().orElse(Collections.emptyList());
        } catch (HttpClientResponseException | ReadTimeoutException e) {
            log.debug("RabbitMQ listQueues failed for {}: {}", url, e.getMessage());
            return Collections.emptyList();
        } catch (HttpClientException e) {
            log.warn("Failed to connect to RabbitMQ {}: {}", url, e.getMessage());
            return Collections.emptyList();
        }
    }

    public RabbitMqQueueInfo getQueue(String baseUrl, String vhost, String queue, String username, String password) {
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/api/queues/" + encodePathSegment(vhost) + "/" + encodePathSegment(queue);
        MutableHttpRequest<?> request = HttpRequest.GET(url)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .basicAuth(username, password);
        try {
            HttpResponse<RabbitMqQueueInfo> response = client.exchange(
                    request,
                    Argument.of(RabbitMqQueueInfo.class)
            );
            return response.getBody().orElse(null);
        } catch (HttpClientResponseException | ReadTimeoutException e) {
            log.debug("RabbitMQ getQueue failed for {}: {}", url, e.getMessage());
            return null;
        } catch (HttpClientException e) {
            log.warn("Failed to connect to RabbitMQ {}: {}", url, e.getMessage());
            return null;
        }
    }

    public List<RabbitMqMessage> getMessages(String baseUrl, String vhost, String queue, int count,
                                             String username, String password) {
        int safeCount = count > 0 ? count : 10;
        RabbitMqGetMessagesRequest body = new RabbitMqGetMessagesRequest(
                safeCount,
                "ack_requeue_true",
                "auto",
                50000
        );
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/api/queues/" + encodePathSegment(vhost) + "/" + encodePathSegment(queue) + "/get";
        MutableHttpRequest<RabbitMqGetMessagesRequest> request = HttpRequest.POST(url, body)
                .contentType(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .basicAuth(username, password);
        try {
            HttpResponse<List<RabbitMqMessage>> response = client.exchange(
                    request,
                    Argument.listOf(RabbitMqMessage.class)
            );
            return response.getBody().orElse(Collections.emptyList());
        } catch (HttpClientResponseException | ReadTimeoutException e) {
            log.debug("RabbitMQ getMessages failed for {}: {}", url, e.getMessage());
            return Collections.emptyList();
        } catch (HttpClientException e) {
            log.warn("Failed to connect to RabbitMQ {}: {}", url, e.getMessage());
            return Collections.emptyList();
        }
    }

    public boolean checkConnection(String baseUrl, String username, String password) {
        BlockingHttpClient client = httpClient.toBlocking();
        String url = baseUrl + "/api/overview";
        MutableHttpRequest<?> request = HttpRequest.GET(url)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .basicAuth(username, password);
        try {
            client.exchange(request, Argument.of(Map.class));
            return true;
        } catch (HttpClientResponseException | ReadTimeoutException e) {
            log.debug("RabbitMQ checkConnection failed for {}: {}", url, e.getMessage());
            return false;
        } catch (HttpClientException e) {
            log.warn("Failed to connect to RabbitMQ {}: {}", url, e.getMessage());
            return false;
        }
    }

    private String encodePathSegment(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}

