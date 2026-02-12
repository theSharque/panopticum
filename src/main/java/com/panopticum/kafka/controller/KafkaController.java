package com.panopticum.kafka.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.kafka.model.KafkaRecord;
import com.panopticum.kafka.model.KafkaTopicInfo;
import com.panopticum.kafka.service.KafkaService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.View;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/kafka")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class KafkaController {

    private static final int DEFAULT_PEEK_COUNT = 20;

    private final DbConnectionService dbConnectionService;
    private final KafkaService kafkaService;

    @Get("/{id}")
    public HttpResponse<?> index(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/kafka/" + id + "/topics"));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/topics")
    @View("kafka/topics")
    public Map<String, Object> topics(@PathVariable Long id,
                                     @QueryValue(value = "page", defaultValue = "1") int page,
                                     @QueryValue(value = "size", defaultValue = "50") int size,
                                     @QueryValue(value = "sort", defaultValue = "name") String sort,
                                     @QueryValue(value = "order", defaultValue = "asc") String order) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);

        Page<KafkaTopicInfo> paged = kafkaService.listTopicsPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "partitionCount", "orderPartitionCount"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/topics/{topic}/partitions")
    @View("kafka/partitions")
    public Map<String, Object> partitions(@PathVariable Long id,
                                          @PathVariable String topic) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String topicDecoded = decodeTopic(topic);

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/kafka/" + id + "/topics"));
        breadcrumbs.add(new BreadcrumbItem(topicDecoded, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("topic", topicDecoded);
        model.put("topicForUrl", topicForUrl(topicDecoded));
        model.put("partitions", kafkaService.getPartitions(id, topicDecoded));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/topics/{topic}/partitions/{partition}/records")
    @View("kafka/records")
    public Map<String, Object> records(@PathVariable Long id,
                                      @PathVariable String topic,
                                      @PathVariable int partition,
                                      @QueryValue(value = "fromOffset", defaultValue = "0") long fromOffset,
                                      @QueryValue(value = "fromEnd", defaultValue = "false") boolean fromEnd,
                                      @QueryValue(value = "count", defaultValue = "20") int count) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String topicDecoded = decodeTopic(topic);
        int peekCount = count > 0 ? Math.min(count, 50) : DEFAULT_PEEK_COUNT;

        List<KafkaRecord> records = fromEnd
                ? kafkaService.peekRecordsFromEnd(id, topicDecoded, partition, peekCount)
                : kafkaService.peekRecords(id, topicDecoded, partition, fromOffset, peekCount);
        model.put("records", kafkaService.truncateRecordValuesForList(records));

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/kafka/" + id + "/topics"));
        breadcrumbs.add(new BreadcrumbItem(topicDecoded, "/kafka/" + id + "/topics/" + topicForUrl(topicDecoded) + "/partitions"));
        breadcrumbs.add(new BreadcrumbItem("Partition " + partition, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("topic", topicDecoded);
        model.put("topicForUrl", topicForUrl(topicDecoded));
        model.put("partition", partition);
        model.put("peekCount", peekCount);
        model.put("fromOffset", fromOffset);
        model.put("fromEnd", fromEnd);

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/topics/{topic}/partitions/{partition}/records/{offset}")
    @View("kafka/record-detail")
    public Map<String, Object> recordDetail(@PathVariable Long id,
                                            @PathVariable String topic,
                                            @PathVariable int partition,
                                            @PathVariable long offset) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String topicDecoded = decodeTopic(topic);

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/kafka/" + id + "/topics"));
        breadcrumbs.add(new BreadcrumbItem(topicDecoded, "/kafka/" + id + "/topics/" + topicForUrl(topicDecoded) + "/partitions"));
        breadcrumbs.add(new BreadcrumbItem("Partition " + partition,
                "/kafka/" + id + "/topics/" + topicForUrl(topicDecoded) + "/partitions/" + partition + "/records"));
        breadcrumbs.add(new BreadcrumbItem("Offset " + offset, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("topic", topicDecoded);
        model.put("topicForUrl", topicForUrl(topicDecoded));
        model.put("partition", partition);
        model.put("offset", offset);

        Optional<KafkaRecord> record = kafkaService.getRecordByOffset(id, topicDecoded, partition, offset);
        model.put("record", record.orElse(null));
        if (record.isEmpty()) {
            model.put("error", "kafka.recordNotFound");
        }

        return model;
    }

    private static String decodeTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            return "";
        }
        try {
            return unquoteIdentifier(URLDecoder.decode(topic, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return unquoteIdentifier(topic);
        }
    }

    private static String topicForUrl(String topic) {
        if (topic == null) {
            return "";
        }
        return topic;
    }

    private static String unquoteIdentifier(String s) {
        if (s == null || s.length() < 2) {
            return s != null ? s : "";
        }
        if (s.charAt(0) == '"' && s.lastIndexOf('"') > 0) {
            int end = s.lastIndexOf('"');
            return s.substring(1, end).replace("\"\"", "\"");
        }
        return s;
    }
}
