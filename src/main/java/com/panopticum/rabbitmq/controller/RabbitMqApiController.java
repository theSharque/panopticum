package com.panopticum.rabbitmq.controller;

import com.panopticum.core.model.Page;
import com.panopticum.core.controller.AbstractConnectionApiController;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.rabbitmq.model.RabbitMqMessage;
import com.panopticum.rabbitmq.model.RabbitMqQueueInfo;
import com.panopticum.rabbitmq.service.RabbitMqService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.exceptions.HttpStatusException;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller("/api/rabbitmq/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@Tag(name = "RabbitMQ", description = "RabbitMQ queues and messages API")
public class RabbitMqApiController extends AbstractConnectionApiController {

    private final RabbitMqService rabbitMqService;
    private final ObjectMapper objectMapper;

    public RabbitMqApiController(DbConnectionService dbConnectionService, RabbitMqService rabbitMqService,
                                 ObjectMapper objectMapper) {
        super(dbConnectionService);
        this.rabbitMqService = rabbitMqService;
        this.objectMapper = objectMapper;
    }

    @Get("/{id}/queues")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List queues")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Queues page"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Page<RabbitMqQueueInfo> queues(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @QueryValue(value = "page", defaultValue = "1") int page,
            @QueryValue(value = "size", defaultValue = "50") int size,
            @QueryValue(value = "sort", defaultValue = "name") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        return rabbitMqService.listQueuesPaged(id, page, size, sort, order);
    }

    @Get("/{id}/queues/{vhost}/{queue}/messages")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List messages")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Messages list (truncated payloads)"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public List<RabbitMqMessage> messages(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String vhost,
            @PathVariable String queue,
            @QueryValue(value = "count", defaultValue = "20") int count,
            @QueryValue(value = "sort", defaultValue = "index") String sort,
            @QueryValue(value = "order", defaultValue = "asc") String order) {
        ensureConnectionExists(id);
        String vhostDecoded = decodeVhost(vhost);
        String queueName = queue != null ? queue : "";
        int peekCount = count > 0 ? Math.min(count, 50) : 20;
        List<RabbitMqMessage> messages = rabbitMqService.peekMessages(id, vhostDecoded, queueName, peekCount);
        List<RabbitMqMessage> sorted = rabbitMqService.sortMessages(messages, sort != null ? sort : "index", order != null ? order : "asc");
        return rabbitMqService.truncatePayloadsForList(sorted);
    }

    @Get("/{id}/queues/{vhost}/{queue}/messages/{index}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get message by index")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Message or null"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public RabbitMqMessage messageByIndex(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String vhost,
            @PathVariable String queue,
            @PathVariable int index) {
        ensureConnectionExists(id);
        String vhostDecoded = decodeVhost(vhost);
        String queueName = queue != null ? queue : "";
        return rabbitMqService.peekOneByIndex(id, vhostDecoded, queueName, index).orElse(null);
    }

    @Post("/{id}/queues/{vhost}/{queue}/publish")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Publish messages to queue")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Publish result"),
            @ApiResponse(responseCode = "400", description = "Empty message list"),
            @ApiResponse(responseCode = "403", description = "read.only.enabled"),
            @ApiResponse(responseCode = "404", description = "connection.notFound")
    })
    public Map<String, Object> publish(
            @Parameter(description = "Connection ID") @PathVariable Long id,
            @PathVariable String vhost,
            @PathVariable String queue,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "Messages to publish (each object serialized to JSON string payload)",
                    content = @Content(
                            examples = @ExampleObject(
                                    name = "messages",
                                    value = "[{\"message\":\"hello\"},{\"id\":1}]"
                            ),
                            schema = @Schema(type = "array")
                    )
            )
            @Body List<Map<String, Object>> messages) {
        assertNotReadOnly();
        ensureConnectionExists(id);
        if (messages == null || messages.isEmpty()) {
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, "rabbitmq.publishRequired");
        }
        String vhostDecoded = decodeVhost(vhost);
        String queueName = queue != null ? queue : "";
        List<String> payloads = messages.stream()
                .map(this::toJsonPayload)
                .collect(Collectors.toList());
        int published = rabbitMqService.publishMessages(id, vhostDecoded, queueName, payloads);

        return Map.of(
                "published", published,
                "requested", payloads.size(),
                "queue", queueName,
                "vhost", vhostDecoded
        );
    }

    private String toJsonPayload(Map<String, Object> message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid message payload");
        }
    }

    private static String decodeVhost(String vhost) {
        if (vhost == null || vhost.isBlank()) {
            return "/";
        }
        return "_".equals(vhost) ? "/" : vhost;
    }
}
