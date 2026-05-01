package com.panopticum.rabbitmq.service;

import com.panopticum.core.error.ConnectionSupport;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.StringUtils;
import com.panopticum.mcp.model.ColumnInfo;
import com.panopticum.mcp.model.EntityDescription;
import com.panopticum.rabbitmq.client.RabbitMqManagementClient;
import com.panopticum.rabbitmq.model.RabbitMqMessage;
import com.panopticum.rabbitmq.model.RabbitMqQueueInfo;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
        DbConnection conn = requireRabbitConnection(connectionId);
        return managementClient.listQueues(
                managementBaseUrl(conn.getHost(), conn.getPort()),
                conn.getUsername() != null ? conn.getUsername() : "",
                conn.getPassword() != null ? conn.getPassword() : "");
    }

    public Page<RabbitMqQueueInfo> listQueuesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<RabbitMqQueueInfo> all = listQueues(connectionId);
        List<RabbitMqQueueInfo> sorted = all.stream()
                .sorted(queueComparator(sort, order))
                .toList();
        return Page.of(sorted, page, size, sort != null ? sort : "name", order != null ? order : "asc");
    }

    private static Comparator<RabbitMqQueueInfo> queueComparator(String sort, String order) {
        String by = sort != null ? sort : "name";
        boolean desc = "desc".equalsIgnoreCase(order);
        Comparator<RabbitMqQueueInfo> c = switch (by) {
            case "vhost" -> Comparator.comparing(q -> nullSafe(q.getVhostForUrl()));
            case "messages" -> Comparator.comparing(q -> nullSafe(q.getMessages()));
            case "messagesReady" -> Comparator.comparing(q -> nullSafe(q.getMessagesReady()));
            case "messagesUnacknowledged" -> Comparator.comparing(q -> nullSafe(q.getMessagesUnacknowledged()));
            case "consumers" -> Comparator.comparing(q -> nullSafe(q.getConsumers()));
            default -> Comparator.comparing(q -> nullSafe(q.getName()));
        };
        return desc ? c.reversed() : c;
    }

    private static String nullSafe(String s) {
        return s != null ? s : "";
    }

    private static Long nullSafe(Long n) {
        return n != null ? n : 0L;
    }

    private static Integer nullSafe(Integer n) {
        return n != null ? n : 0;
    }

    public Optional<RabbitMqQueueInfo> getQueueDetails(Long connectionId, String vhost, String queue) {
        DbConnection conn = requireRabbitConnection(connectionId);
        return Optional.ofNullable(managementClient.getQueue(
                        managementBaseUrl(conn.getHost(), conn.getPort()),
                        vhost, queue,
                        conn.getUsername() != null ? conn.getUsername() : "",
                        conn.getPassword() != null ? conn.getPassword() : ""));
    }

    public List<RabbitMqMessage> peekMessages(Long connectionId, String vhost, String queue, int count) {
        int safeCount = count > 0 ? Math.min(count, PEEK_MAX_COUNT) : defaultPeekCount;
        DbConnection conn = requireRabbitConnection(connectionId);
        return managementClient.getMessages(
                managementBaseUrl(conn.getHost(), conn.getPort()),
                vhost, queue, safeCount,
                conn.getUsername() != null ? conn.getUsername() : "",
                conn.getPassword() != null ? conn.getPassword() : "");
    }

    private DbConnection requireRabbitConnection(Long connectionId) {
        return ConnectionSupport.require(
                dbConnectionService.findById(connectionId).filter(c -> "rabbitmq".equalsIgnoreCase(c.getType())));
    }

    public List<RabbitMqMessage> sortMessages(List<RabbitMqMessage> messages, String sort, String order) {
        if (messages == null || messages.isEmpty()) {
            return List.of();
        }
        boolean desc = "desc".equalsIgnoreCase(order);
        Comparator<RabbitMqMessage> c = switch (sort != null ? sort : "index") {
            case "routingKey" -> Comparator.comparing(m -> m.getRoutingKey() != null ? m.getRoutingKey() : "");
            case "payload" -> Comparator.comparing(m -> m.getPayload() != null ? m.getPayload() : "");
            default -> null;
        };
        if (c != null) {
            return (desc ? messages.stream().sorted(c.reversed()) : messages.stream().sorted(c)).toList();
        }
        if (desc) {
            List<RabbitMqMessage> copy = new ArrayList<>(messages);
            Collections.reverse(copy);
            return copy;
        }
        return messages;
    }

    public List<RabbitMqMessage> truncatePayloadsForList(List<RabbitMqMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return List.of();
        }
        return messages.stream()
                .map(m -> RabbitMqMessage.builder()
                        .routingKey(m.getRoutingKey())
                        .payloadBytes(m.getPayloadBytes())
                        .properties(m.getProperties())
                        .payload((String) StringUtils.truncateCell(m.getPayload()))
                        .build())
                .toList();
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

    public Optional<EntityDescription> describeQueue(Long connectionId, String catalog, String queueName) {
        try {
            DbConnection conn = requireRabbitConnection(connectionId);
            String baseUrl = managementBaseUrl(conn.getHost(), conn.getPort());
            String vhost = catalog != null && !catalog.isBlank() ? catalog : conn.getDbName();
            if (vhost == null || vhost.isBlank()) vhost = "/";
            String user = conn.getUsername() != null ? conn.getUsername() : "";
            String pass = conn.getPassword() != null ? conn.getPassword() : "";
            String finalVhost = vhost;
            List<RabbitMqQueueInfo> queues = managementClient.listQueues(baseUrl, user, pass).stream()
                    .filter(q -> finalVhost.equals(q.getVhost()))
                    .toList();
            RabbitMqQueueInfo info = queues.stream().filter(q -> queueName.equals(q.getName())).findFirst().orElse(null);
            if (info == null) return Optional.empty();

            List<ColumnInfo> columns = List.of(
                    ColumnInfo.builder().name("routing_key").type("string").nullable(false).primaryKey(true).position(1).build(),
                    ColumnInfo.builder().name("payload").type("bytes/string").nullable(true).primaryKey(false).position(2).build(),
                    ColumnInfo.builder().name("content_type").type("string").nullable(true).primaryKey(false).position(3).build(),
                    ColumnInfo.builder().name("delivery_mode").type("int").nullable(true).primaryKey(false).position(4).build()
            );
            List<String> notes = new ArrayList<>();
            notes.add("messages=" + (info.getMessages() != null ? info.getMessages() : 0));
            notes.add("consumers=" + (info.getConsumers() != null ? info.getConsumers() : 0));
            notes.add("vhost=" + vhost);

            return Optional.of(EntityDescription.builder()
                    .entityKind("queue")
                    .catalog(vhost)
                    .namespace(null)
                    .entity(queueName)
                    .columns(columns)
                    .primaryKey(List.of())
                    .foreignKeys(List.of())
                    .indexes(List.of())
                    .approximateRowCount(info.getMessages())
                    .inferredFromSample(false)
                    .notes(notes)
                    .build());
        } catch (Exception e) {
            log.warn("describeQueue failed for {}: {}", queueName, e.getMessage());
            return Optional.empty();
        }
    }
}
