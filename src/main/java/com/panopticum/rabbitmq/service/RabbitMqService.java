package com.panopticum.rabbitmq.service;

import com.panopticum.core.service.DbConnectionService;
import com.panopticum.rabbitmq.client.RabbitMqManagementClient;
import com.panopticum.rabbitmq.model.RabbitMqMessage;
import com.panopticum.rabbitmq.model.RabbitMqQueueInfo;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class RabbitMqService {

    private static final int PEEK_MAX_COUNT = 50;

    private final DbConnectionService dbConnectionService;
    private final RabbitMqManagementClient managementClient;

    @Value("${panopticum.limits.rabbitmq.peek-count:20}")
    private int defaultPeekCount;

    public Optional<String> testConnection(String host, int port, String vhost, String username, String password) {
        String baseUrl = managementBaseUrl(host, port);
        boolean ok = managementClient.checkConnection(baseUrl, username != null ? username : "", password != null ? password : "");
        return ok ? Optional.empty() : Optional.of("connectionTest.failed");
    }

    public List<RabbitMqQueueInfo> listQueues(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .map(conn -> managementClient.listQueues(
                        managementBaseUrl(conn.getHost(), conn.getPort()),
                        conn.getUsername() != null ? conn.getUsername() : "",
                        conn.getPassword() != null ? conn.getPassword() : ""))
                .orElse(List.of());
    }

    public Optional<RabbitMqQueueInfo> getQueueDetails(Long connectionId, String vhost, String queue) {
        return dbConnectionService.findById(connectionId)
                .map(conn -> managementClient.getQueue(
                        managementBaseUrl(conn.getHost(), conn.getPort()),
                        vhost, queue,
                        conn.getUsername() != null ? conn.getUsername() : "",
                        conn.getPassword() != null ? conn.getPassword() : ""))
                .filter(q -> q != null);
    }

    public List<RabbitMqMessage> peekMessages(Long connectionId, String vhost, String queue, int count) {
        int safeCount = count > 0 ? Math.min(count, PEEK_MAX_COUNT) : defaultPeekCount;
        return dbConnectionService.findById(connectionId)
                .map(conn -> managementClient.getMessages(
                        managementBaseUrl(conn.getHost(), conn.getPort()),
                        vhost, queue, safeCount,
                        conn.getUsername() != null ? conn.getUsername() : "",
                        conn.getPassword() != null ? conn.getPassword() : ""))
                .orElse(List.of());
    }

    public Optional<RabbitMqMessage> peekOneByIndex(Long connectionId, String vhost, String queue, int index) {
        if (index < 0) {
            return Optional.empty();
        }
        int count = index + 1;
        List<RabbitMqMessage> messages = peekMessages(connectionId, vhost, queue, count);
        if (index < messages.size()) {
            return Optional.of(messages.get(index));
        }
        return Optional.empty();
    }

    private static String managementBaseUrl(String host, int port) {
        String h = host != null && !host.isBlank() ? host : "localhost";
        int p = port > 0 ? port : 15672;
        return "http://" + h + ":" + p;
    }
}
