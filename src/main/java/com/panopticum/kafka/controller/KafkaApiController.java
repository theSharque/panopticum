package com.panopticum.kafka.controller;

import com.panopticum.core.model.Page;
import com.panopticum.core.controller.AbstractConnectionApiController;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.kafka.model.KafkaPartitionInfo;
import com.panopticum.kafka.model.KafkaRecord;
import com.panopticum.kafka.model.KafkaTopicInfo;
import com.panopticum.kafka.service.KafkaService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.MediaType;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Controller("/api/kafka/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@Tag(name = "Kafka", description = "Kafka topics, partitions and records API")
public class KafkaApiController extends AbstractConnectionApiController {

    private final KafkaService kafkaService;

    public KafkaApiController(DbConnectionService dbConnectionService, KafkaService kafkaService) {
        super(dbConnectionService);
        this.kafkaService = kafkaService;
    }

    @Get("/{id}/topics")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List topics")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Topics page"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<KafkaTopicInfo> topics(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return kafkaService.listTopicsPaged(id, page, size, sort, order);
    }

    @Get("/{id}/topics/{topic}/partitions")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List partitions")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Partition list"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public List<KafkaPartitionInfo> partitions(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String topic) {
        ensureConnectionExists(id);
        return kafkaService.getPartitions(id, decodeTopic(topic));
    }

    @Get("/{id}/topics/{topic}/partitions/{partition}/records")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List records")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Records list (truncated values)"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public List<KafkaRecord> records(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String topic,
            @PathVariable int partition,
            @QueryValue(value = "fromOffset", defaultValue = "0") long fromOffset,
            @QueryValue(value = "fromEnd", defaultValue = "false") boolean fromEnd,
            @QueryValue(value = "count", defaultValue = "20") int count) {
        ensureConnectionExists(id);
        String topicDecoded = decodeTopic(topic);
        List<KafkaRecord> records = fromEnd
                ? kafkaService.peekRecordsFromEnd(id, topicDecoded, partition, count > 0 ? Math.min(count, 50) : 20)
                : kafkaService.peekRecords(id, topicDecoded, partition, fromOffset, count > 0 ? Math.min(count, 50) : 20);
        return kafkaService.truncateRecordValuesForList(records);
    }

    @Get("/{id}/topics/{topic}/partitions/{partition}/records/{offset}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get record by offset")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Record or null"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public KafkaRecord recordByOffset(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String topic,
            @PathVariable int partition,
            @PathVariable long offset) {
        ensureConnectionExists(id);
        return kafkaService.getRecordByOffset(id, decodeTopic(topic), partition, offset).orElse(null);
    }

    private static String decodeTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            return "";
        }
        try {
            return URLDecoder.decode(topic, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return topic;
        }
    }
}
