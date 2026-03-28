package com.panopticum.rabbitmq.controller;

import com.panopticum.core.model.Page;
import com.panopticum.core.controller.AbstractConnectionApiController;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.rabbitmq.model.RabbitMqMessage;
import com.panopticum.rabbitmq.model.RabbitMqQueueInfo;
import com.panopticum.rabbitmq.service.RabbitMqService;
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

import java.util.List;

@Controller("/api/rabbitmq/connections")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@Tag(name = "RabbitMQ", description = "RabbitMQ queues and messages API")
public class RabbitMqApiController extends AbstractConnectionApiController {

    private final RabbitMqService rabbitMqService;

    public RabbitMqApiController(DbConnectionService dbConnectionService, RabbitMqService rabbitMqService) {
        super(dbConnectionService);
        this.rabbitMqService = rabbitMqService;
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

    private static String decodeVhost(String vhost) {
        if (vhost == null || vhost.isBlank()) {
            return "/";
        }
        return "_".equals(vhost) ? "/" : vhost;
    }
}
