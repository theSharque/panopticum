package com.panopticum.kafka.client;

import com.panopticum.kafka.model.KafkaPartitionInfo;
import com.panopticum.kafka.model.KafkaRecord;
import com.panopticum.kafka.model.KafkaTopicInfo;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Singleton
@Slf4j
public class KafkaClient {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(3000);

    public boolean checkConnection(String bootstrapServers) {
        log.info("Kafka checkConnection: connecting to bootstrap={}", bootstrapServers);
        try (Admin admin = adminClient(bootstrapServers)) {
            ListTopicsResult result = admin.listTopics();
            result.names().get();
            log.info("Kafka checkConnection: OK, listTopics succeeded for {}", bootstrapServers);
            return true;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            String msg = cause.getMessage();
            log.warn("Kafka checkConnection failed for {}: {} - {}", bootstrapServers, cause.getClass().getSimpleName(), msg);
            if (log.isDebugEnabled()) {
                log.debug("Kafka checkConnection ExecutionException", e);
            }
            return false;
        } catch (Exception e) {
            log.warn("Kafka checkConnection failed for {}: {} - {}", bootstrapServers, e.getClass().getSimpleName(), e.getMessage());
            if (log.isDebugEnabled()) {
                log.debug("Kafka checkConnection exception", e);
            }
            return false;
        }
    }

    public List<KafkaTopicInfo> listTopics(String bootstrapServers) {
        try (Admin admin = adminClient(bootstrapServers)) {
            ListTopicsResult listResult = admin.listTopics();
            Set<String> names = listResult.names().get();
            if (names == null || names.isEmpty()) {
                return List.of();
            }
            DescribeTopicsResult describeResult = admin.describeTopics(names);
            Map<String, TopicDescription> descriptions = describeResult.allTopicNames().get();
            List<KafkaTopicInfo> topics = new ArrayList<>();
            for (String name : names) {
                KafkaTopicInfo info = new KafkaTopicInfo();
                info.setName(name);
                TopicDescription desc = descriptions.get(name);
                info.setPartitionCount(desc != null ? desc.partitions().size() : 0);
                topics.add(info);
            }
            return topics;
        } catch (ExecutionException e) {
            log.debug("Kafka listTopics failed for {}: {}", bootstrapServers, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            return List.of();
        } catch (Exception e) {
            log.warn("Failed to list Kafka topics {}: {}", bootstrapServers, e.getMessage());
            return List.of();
        }
    }

    public List<KafkaPartitionInfo> getPartitions(String bootstrapServers, String topic) {
        try (Admin admin = adminClient(bootstrapServers)) {
            DescribeTopicsResult result = admin.describeTopics(Collections.singletonList(topic));
            TopicDescription desc = result.allTopicNames().get().get(topic);
            if (desc == null) {
                return List.of();
            }
            List<KafkaPartitionInfo> partitions = new ArrayList<>();
            for (TopicPartitionInfo pi : desc.partitions()) {
                KafkaPartitionInfo info = new KafkaPartitionInfo();
                info.setTopic(topic);
                info.setPartition(pi.partition());
                partitions.add(info);
            }
            return partitions;
        } catch (ExecutionException e) {
            log.debug("Kafka getPartitions failed for {} {}: {}", bootstrapServers, topic, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            return List.of();
        } catch (Exception e) {
            log.warn("Failed to get partitions for {} {}: {}", bootstrapServers, topic, e.getMessage());
            return List.of();
        }
    }

    public List<KafkaRecord> peekRecords(String bootstrapServers, String topic, int partition, long fromOffset, int count) {
        TopicPartition tp = new TopicPartition(topic, partition);
        Map<String, Object> props = consumerProps(bootstrapServers);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(Collections.singletonList(tp));
            consumer.seek(tp, fromOffset);
            List<KafkaRecord> records = new ArrayList<>();
            int remaining = count;
            while (remaining > 0) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(POLL_TIMEOUT);
                if (batch.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<byte[], byte[]> rec : batch.records(tp)) {
                    if (remaining <= 0) {
                        break;
                    }
                    records.add(toKafkaRecord(rec));
                    remaining--;
                }
            }
            return records;
        } catch (Exception e) {
            log.warn("Failed to peek records for {} {}:{}: {}", bootstrapServers, topic, partition, e.getMessage());
            return List.of();
        }
    }

    public List<KafkaRecord> peekRecordsFromEnd(String bootstrapServers, String topic, int partition, int count) {
        TopicPartition tp = new TopicPartition(topic, partition);
        Map<String, Object> props = consumerProps(bootstrapServers);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(Collections.singletonList(tp));
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(tp));
            Long end = endOffsets.get(tp);
            if (end == null || end == 0) {
                return List.of();
            }
            long fromOffset = Math.max(0, end - count);
            consumer.seek(tp, fromOffset);
            List<KafkaRecord> records = new ArrayList<>();
            int remaining = (int) Math.min(count, end - fromOffset);
            while (remaining > 0) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(POLL_TIMEOUT);
                if (batch.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<byte[], byte[]> rec : batch.records(tp)) {
                    if (remaining <= 0) {
                        break;
                    }
                    records.add(toKafkaRecord(rec));
                    remaining--;
                }
            }
            return records;
        } catch (Exception e) {
            log.warn("Failed to peek records from end for {} {}:{}: {}", bootstrapServers, topic, partition, e.getMessage());
            return List.of();
        }
    }

    private static KafkaRecord toKafkaRecord(ConsumerRecord<byte[], byte[]> rec) {
        return KafkaRecord.builder()
                .offset(rec.offset())
                .partition(rec.partition())
                .key(bytesToString(rec.key()))
                .value(bytesToString(rec.value()))
                .timestamp(rec.timestamp() >= 0 ? rec.timestamp() : null)
                .headers(rec.headers() != null ? headersToMap(rec) : null)
                .build();
    }

    private static String bytesToString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "[binary " + bytes.length + " bytes]";
        }
    }

    private static Map<String, String> headersToMap(ConsumerRecord<byte[], byte[]> rec) {
        if (rec.headers() == null) {
            return null;
        }
        Map<String, String> map = new HashMap<>();
        rec.headers().forEach(h -> map.put(h.key(), h.value() != null ? new String(h.value(), StandardCharsets.UTF_8) : ""));
        return map;
    }

    private Admin adminClient(String bootstrapServers) {
        return Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000
        ));
    }

    private Map<String, Object> consumerProps(String bootstrapServers) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "panopticum-peek-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
    }
}
