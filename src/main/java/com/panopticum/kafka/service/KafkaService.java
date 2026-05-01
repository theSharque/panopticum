package com.panopticum.kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.core.error.ConnectionSupport;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.StringUtils;
import com.panopticum.kafka.client.KafkaClient;
import com.panopticum.kafka.model.KafkaPartitionInfo;
import com.panopticum.kafka.model.KafkaRecord;
import com.panopticum.kafka.model.KafkaTopicInfo;
import com.panopticum.mcp.model.ColumnInfo;
import com.panopticum.mcp.model.EntityDescription;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class KafkaService {

    private static final int PEEK_MAX_COUNT = 50;
    private static final int EXECUTE_QUERY_HARD_LIMIT = 100;

    private final DbConnectionService dbConnectionService;
    private final KafkaClient kafkaClient;
    private final ObjectMapper objectMapper;

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
        DbConnection conn = requireKafkaConnection(connectionId);
        return kafkaClient.listTopics(bootstrapServers(conn.getHost(), conn.getPort()));
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
        DbConnection conn = requireKafkaConnection(connectionId);
        return kafkaClient.getPartitions(bootstrapServers(conn.getHost(), conn.getPort()), topic);
    }

    public List<KafkaRecord> peekRecords(Long connectionId, String topic, int partition, long fromOffset, int count) {
        int safeCount = count > 0 ? Math.min(count, PEEK_MAX_COUNT) : defaultPeekCount;
        DbConnection conn = requireKafkaConnection(connectionId);
        return kafkaClient.peekRecords(bootstrapServers(conn.getHost(), conn.getPort()), topic, partition, fromOffset, safeCount);
    }

    public List<KafkaRecord> peekRecordsFromEnd(Long connectionId, String topic, int partition, int count) {
        int safeCount = count > 0 ? Math.min(count, PEEK_MAX_COUNT) : defaultPeekCount;
        DbConnection conn = requireKafkaConnection(connectionId);
        return kafkaClient.peekRecordsFromEnd(bootstrapServers(conn.getHost(), conn.getPort()), topic, partition, safeCount);
    }

    private DbConnection requireKafkaConnection(Long connectionId) {
        return ConnectionSupport.require(
                dbConnectionService.findById(connectionId).filter(c -> "kafka".equalsIgnoreCase(c.getType())));
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

    public Optional<QueryResult> executeQuery(Long connectionId, String topic, String entity, String query,
                                              int offset, int effectiveLimit) {
        if (topic == null || topic.isBlank()) {
            return Optional.of(QueryResult.error("catalog (topic name) is required for Kafka"));
        }
        int partition = 0;
        long fromOffset = 0L;
        int count = Math.min(effectiveLimit, EXECUTE_QUERY_HARD_LIMIT);
        boolean fromEnd = false;
        if (query != null && !query.isBlank()) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> opts = objectMapper.readValue(query.trim(), Map.class);
                if (opts.containsKey("partition")) {
                    Object p = opts.get("partition");
                    partition = p instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(p));
                }
                if (opts.containsKey("fromOffset")) {
                    Object o = opts.get("fromOffset");
                    fromOffset = o instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(o));
                }
                if (opts.containsKey("count")) {
                    Object c = opts.get("count");
                    count = Math.min(c instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(c)), EXECUTE_QUERY_HARD_LIMIT);
                }
                if (opts.containsKey("fromEnd") && Boolean.TRUE.equals(opts.get("fromEnd"))) {
                    fromEnd = true;
                }
            } catch (Exception e) {
                log.debug("Kafka query JSON parse failed, using defaults: {}", e.getMessage());
            }
        }
        if (entity != null && !entity.isBlank()) {
            try {
                partition = Integer.parseInt(entity.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        List<KafkaRecord> records = fromEnd
                ? peekRecordsFromEnd(connectionId, topic, partition, count)
                : peekRecords(connectionId, topic, partition, fromOffset, count);
        records = truncateRecordValuesForList(records);
        List<String> columns = List.of("offset", "partition", "key", "value", "timestamp");
        List<List<Object>> rows = records.stream()
                .map(r -> List.<Object>of(
                        r.getOffset(),
                        r.getPartition(),
                        r.getKey(),
                        r.getValue(),
                        r.getTimestamp()))
                .toList();
        boolean hasMore = records.size() >= count;
        return Optional.of(new QueryResult(columns, null, rows, null, null, offset, count, hasMore));
    }

    private static String bootstrapServers(String host, int port) {
        String h = host != null && !host.isBlank() ? host : "localhost";
        int p = port > 0 ? port : 9092;
        return h + ":" + p;
    }

    public Optional<EntityDescription> describeEntity(Long connectionId, String topicName) {
        try {
            DbConnection conn = requireKafkaConnection(connectionId);
            String bootstrap = bootstrapServers(conn.getHost(), conn.getPort());
            List<KafkaPartitionInfo> partitions = kafkaClient.getPartitions(bootstrap, topicName);
            List<ColumnInfo> columns = List.of(
                    ColumnInfo.builder().name("offset").type("int64").nullable(false).primaryKey(true).position(1).build(),
                    ColumnInfo.builder().name("partition").type("int32").nullable(false).primaryKey(true).position(2).build(),
                    ColumnInfo.builder().name("key").type("bytes/string").nullable(true).primaryKey(false).position(3).build(),
                    ColumnInfo.builder().name("value").type("bytes/string").nullable(true).primaryKey(false).position(4).build(),
                    ColumnInfo.builder().name("timestamp").type("int64").nullable(false).primaryKey(false).position(5).build()
            );
            List<String> notes = new ArrayList<>();
            notes.add("partitions=" + partitions.size());
            notes.add("schemaRegistry=disabled");
            partitions.stream().map(p -> "partition=" + p.getPartition()).forEach(notes::add);

            return Optional.of(EntityDescription.builder()
                    .entityKind("topic")
                    .catalog(topicName)
                    .namespace(null)
                    .entity(topicName)
                    .columns(columns)
                    .primaryKey(List.of("partition", "offset"))
                    .foreignKeys(List.of())
                    .indexes(List.of())
                    .approximateRowCount(null)
                    .inferredFromSample(false)
                    .notes(notes)
                    .build());
        } catch (Exception e) {
            log.warn("describeEntity failed for topic {}: {}", topicName, e.getMessage());
            return Optional.empty();
        }
    }
}
