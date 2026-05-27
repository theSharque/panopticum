package com.panopticum.rabbitmq.controller;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.ui.AppAlerts;
import com.panopticum.core.util.ControllerModelHelper;
import com.panopticum.rabbitmq.model.RabbitMqMessage;
import com.panopticum.rabbitmq.model.RabbitMqQueueInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.rabbitmq.service.RabbitMqService;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.ModelAndView;
import io.micronaut.views.View;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/rabbitmq")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class RabbitMqController {

    private static final int DEFAULT_PEEK_COUNT = 20;

    private final DbConnectionService dbConnectionService;
    private final RabbitMqService rabbitMqService;
    private final ObjectMapper objectMapper;

    @Value("${panopticum.read-only:false}")
    private boolean readOnly;

    @Get("/{id}")
    public HttpResponse<?> index(@PathVariable Long id) {
        return HttpResponse.redirect(URI.create("/rabbitmq/" + id + "/queues"));
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/queues")
    @View("rabbitmq/queues")
    public Map<String, Object> queues(@PathVariable Long id,
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

        Page<RabbitMqQueueInfo> paged = rabbitMqService.listQueuesPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "vhost", "orderVhost", "messages", "orderMessages",
                        "messagesReady", "orderMessagesReady", "messagesUnacknowledged", "orderMessagesUnack",
                        "consumers", "orderConsumers"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/queues/{vhost}/{queue}/messages")
    @View("rabbitmq/messages")
    public Map<String, Object> messages(@PathVariable Long id,
                                       @PathVariable String vhost,
                                       @PathVariable String queue,
                                       @QueryValue(value = "count", defaultValue = "20") int count,
                                       @QueryValue(value = "sort", defaultValue = "index") String sort,
                                       @QueryValue(value = "order", defaultValue = "asc") String order) {
        return messagesModel(id, vhost, queue, count, sort, order, null);
    }

    @Post("/{id}/queues/{vhost}/{queue}/publish")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object publish(@PathVariable Long id,
                          @PathVariable String vhost,
                          @PathVariable String queue,
                          String payloads,
                          @QueryValue(value = "count", defaultValue = "20") int count,
                          @QueryValue(value = "sort", defaultValue = "index") String sort,
                          @QueryValue(value = "order", defaultValue = "asc") String order) {
        assertNotReadOnly();

        List<String> payloadList;
        try {
            payloadList = parsePublishPayloads(payloads);
        } catch (IllegalArgumentException e) {
            Map<String, Object> model = messagesModel(id, vhost, queue, count, sort, order, payloads);
            AppAlerts.i18n(model, "rabbitmq.publishRequired");

            return new ModelAndView<>("rabbitmq/messages", model);
        }

        if (payloadList.isEmpty()) {
            Map<String, Object> model = messagesModel(id, vhost, queue, count, sort, order, payloads);
            AppAlerts.i18n(model, "rabbitmq.publishRequired");

            return new ModelAndView<>("rabbitmq/messages", model);
        }

        String vhostDecoded = decodeVhost(vhost);
        String queueName = queue != null ? queue : "";

        try {
            int published = rabbitMqService.publishMessages(id, vhostDecoded, queueName, payloadList);
            Map<String, Object> model = messagesModel(id, vhost, queue, count, sort, order, null);
            AppAlerts.raw(model, published + " / " + payloadList.size());

            return new ModelAndView<>("rabbitmq/messages", model);
        } catch (Exception e) {
            Map<String, Object> model = messagesModel(id, vhost, queue, count, sort, order, payloads);
            AppAlerts.fromControllerMessage(model, e.getMessage() != null ? e.getMessage() : "rabbitmq.publishFailed");

            return new ModelAndView<>("rabbitmq/messages", model);
        }
    }

    private Map<String, Object> messagesModel(Long id, String vhost, String queue, int count, String sort, String order,
                                              String publishDraft) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String vhostDecoded = decodeVhost(vhost);
        String queueName = queue != null ? queue : "";

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/rabbitmq/" + id + "/queues"));
        breadcrumbs.add(new BreadcrumbItem(queueBreadcrumbLabel(vhostDecoded, queueName), null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("vhost", vhostDecoded);
        model.put("queue", queueName);
        model.put("queueDetails", rabbitMqService.getQueueDetails(id, vhostDecoded, queueName).orElse(null));
        model.put("readOnly", readOnly);
        model.put("publishDraft", publishDraft != null ? publishDraft : "");

        int peekCount = count > 0 ? Math.min(count, 50) : DEFAULT_PEEK_COUNT;
        List<RabbitMqMessage> messages = rabbitMqService.peekMessages(id, vhostDecoded, queueName, peekCount);
        List<RabbitMqMessage> sorted = rabbitMqService.sortMessages(messages, sort != null ? sort : "index", order != null ? order : "asc");
        model.put("messages", rabbitMqService.truncatePayloadsForList(sorted));
        model.put("peekCount", peekCount);
        model.put("vhostForUrl", vhostForUrl(vhostDecoded));
        model.put("sort", sort != null ? sort : "index");
        model.put("order", order != null ? order : "asc");
        boolean asc = "asc".equalsIgnoreCase(order != null ? order : "asc");
        String sortBy = sort != null ? sort : "index";
        model.put("orderIndex", "index".equals(sortBy) && asc ? "desc" : "asc");
        model.put("orderRoutingKey", "routingKey".equals(sortBy) && asc ? "desc" : "asc");
        model.put("orderPayload", "payload".equals(sortBy) && asc ? "desc" : "asc");

        return model;
    }

    private List<String> parsePublishPayloads(String payloads) throws IllegalArgumentException {
        if (payloads == null || payloads.isBlank()) {
            return List.of();
        }

        String trimmed = payloads.trim();
        try {
            JsonNode node = objectMapper.readTree(trimmed);
            if (node.isArray()) {
                List<String> result = new ArrayList<>();
                for (JsonNode item : node) {
                    result.add(objectMapper.writeValueAsString(item));
                }

                return result;
            }
        } catch (JsonProcessingException ignored) {
        }

        List<String> lines = new ArrayList<>();
        for (String line : trimmed.split("\n")) {
            if (line != null && !line.isBlank()) {
                lines.add(line.trim());
            }
        }

        return lines;
    }

    private void assertNotReadOnly() {
        if (readOnly) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "read.only.enabled");
        }
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/queues/{vhost}/{queue}/messages/{index}")
    @View("rabbitmq/message-detail")
    public Map<String, Object> messageDetail(@PathVariable Long id,
                                            @PathVariable String vhost,
                                            @PathVariable String queue,
                                            @PathVariable int index) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        String vhostDecoded = decodeVhost(vhost);
        String queueName = queue != null ? queue : "";

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/rabbitmq/" + id + "/queues"));
        breadcrumbs.add(new BreadcrumbItem(queueBreadcrumbLabel(vhostDecoded, queueName),
                "/rabbitmq/" + id + "/queues/" + vhostForUrl(vhostDecoded) + "/" + queueName + "/messages"));
        breadcrumbs.add(new BreadcrumbItem("Message #" + (index + 1), null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("vhost", vhostDecoded);
        model.put("queue", queueName);
        model.put("index", index);

        Optional<RabbitMqMessage> message = rabbitMqService.peekOneByIndex(id, vhostDecoded, queueName, index);
        model.put("message", message.orElse(null));
        model.put("vhostForUrl", vhostForUrl(vhostDecoded));
        if (message.isEmpty()) {
            AppAlerts.i18n(model, "rabbitmq.messageNotFound");
        } else {
            String label = conn.get().getName() + " / " + queueBreadcrumbLabel(vhostDecoded, queueName) + " / #" + (index + 1);
            try {
                String dataJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(message.get());
                Map<String, Object> payload = Map.of(
                        "source", "rabbitmq",
                        "connectionId", id,
                        "connectionName", conn.get().getName(),
                        "label", label,
                        "data", dataJson,
                        "dataFormat", "json"
                );
                model.put("dataDiffPayload", objectMapper.writeValueAsString(payload));
            } catch (JsonProcessingException e) {
                model.put("dataDiffPayload", (String) null);
            }
        }

        return model;
    }

    private static String decodeVhost(String vhost) {
        if (vhost == null || vhost.isBlank()) {
            return "/";
        }
        return "_".equals(vhost) ? "/" : vhost;
    }

    private static String queueBreadcrumbLabel(String vhostDecoded, String queueName) {
        return "/".equals(vhostDecoded) ? queueName : (vhostForUrl(vhostDecoded) + " / " + queueName);
    }

    private static String vhostForUrl(String vhost) {
        if (vhost == null || vhost.isBlank()) {
            return "_";
        }
        return vhost.replace("/", "_");
    }
}
