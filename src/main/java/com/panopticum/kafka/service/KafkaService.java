package com.panopticum.kafka.service;

import com.panopticum.core.model.Page;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.StringUtils;
import com.panopticum.kafka.client.KafkaClient;
import com.panopticum.kafka.model.KafkaPartitionInfo;
import com.panopticum.kafka.model.KafkaRecord;
import com.panopticum.kafka.model.KafkaTopicInfo;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class KafkaService {

    private static final int PEEK_MAX_COUNT = 50;

    private final DbConnectionService dbConnectionService;
    private final KafkaClient kafkaClient;

    @Value("${panopticum.limits.kafka.peek-count:20}")
    private int defaultPeekCount;

    public Optional<String> testConnection(String host, int port, String database, String username, String password) {
        String bootstrap = bootstrapServers(host, port);
        log.debug("Kafka testConnection: bootstrap={}", bootstrap);
        boolean ok = kafkaClient.checkConnection(bootstrap);
        if (!ok) {
            log.warn("Kafka testConnection failed: bootstrap={}", bootstrap);
        }
        return ok ? Optional.empty() : Optional.of("connectionTest.failed");
    }

    public List<KafkaTopicInfo> listTopics(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .map(conn -> kafkaClient.listTopics(bootstrapServers(conn.getHost(), conn.getPort())))
                .orElse(List.of());
    }

    public Page<KafkaTopicInfo> listTopicsPaged(Long connectionId, int page, int size, String sort, String order) {
        List<KafkaTopicInfo> all = listTopics(connectionId);
        List<KafkaTopicInfo> sorted = all.stream()
                .sorted(topicComparator(sort, order))
                .toList();
        return Page.of(sorted, page, size, sort != null ? sort : "name", order != null ? order : "asc");
    }

    private static Comparator<KafkaTopicInfo> topicComparator(String sort, String order) {
        String by = sort != null ? sort : "name";
        boolean desc = "desc".equalsIgnoreCase(order);
        Comparator<KafkaTopicInfo> c = switch (by) {
            case "partitionCount" -> Comparator.comparingInt(KafkaTopicInfo::getPartitionCount);
            default -> Comparator.comparing(t -> nullSafe(t.getName()));
        };
        return desc ? c.reversed() : c;
    }

    private static String nullSafe(String s) {
        return s != null ? s : "";
    }

    public List<KafkaPartitionInfo> getPartitions(Long connectionId, String topic) {
        return dbConnectionService.findById(connectionId)
                .map(conn -> kafkaClient.getPartitions(bootstrapServers(conn.getHost(), conn.getPort()), topic))
                .orElse(List.of());
    }

    public List<KafkaRecord> peekRecords(Long connectionId, String topic, int partition, long fromOffset, int count) {
        int safeCount = count > 0 ? Math.min(count, PEEK_MAX_COUNT) : defaultPeekCount;
        return dbConnectionService.findById(connectionId)
                .map(conn -> kafkaClient.peekRecords(bootstrapServers(conn.getHost(), conn.getPort()), topic, partition, fromOffset, safeCount))
                .orElse(List.of());
    }

    public List<KafkaRecord> peekRecordsFromEnd(Long connectionId, String topic, int partition, int count) {
        int safeCount = count > 0 ? Math.min(count, PEEK_MAX_COUNT) : defaultPeekCount;
        return dbConnectionService.findById(connectionId)
                .map(conn -> kafkaClient.peekRecordsFromEnd(bootstrapServers(conn.getHost(), conn.getPort()), topic, partition, safeCount))
                .orElse(List.of());
    }

    public Optional<KafkaRecord> getRecordByOffset(Long connectionId, String topic, int partition, long offset) {
        List<KafkaRecord> records = peekRecords(connectionId, topic, partition, offset, 1);
        return records.isEmpty() ? Optional.empty() : Optional.of(records.get(0));
    }

    public List<KafkaRecord> truncateRecordValuesForList(List<KafkaRecord> records) {
        if (records == null || records.isEmpty()) {
            return List.of();
        }
        return records.stream()
                .map(r -> KafkaRecord.builder()
                        .offset(r.getOffset())
                        .partition(r.getPartition())
                        .key(r.getKey() != null ? (String) StringUtils.truncateCell(r.getKey()) : null)
                        .value(r.getValue() != null ? (String) StringUtils.truncateCell(r.getValue()) : null)
                        .timestamp(r.getTimestamp())
                        .headers(r.getHeaders())
                        .build())
                .toList();
    }

    private static String bootstrapServers(String host, int port) {
        String h = host != null && !host.isBlank() ? host : "localhost";
        int p = port > 0 ? port : 9092;
        return h + ":" + p;
    }
}
