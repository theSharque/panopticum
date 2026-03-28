package com.panopticum.core.controller;

import com.panopticum.core.model.DbConnection;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.MediaType;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.util.List;

@Controller("/api")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@Tag(name = "Home", description = "API root")
public class HomeApiController {

    private final ConnectionsApiController connectionsApiController;

    public HomeApiController(ConnectionsApiController connectionsApiController) {
        this.connectionsApiController = connectionsApiController;
    }

    @Get
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List all connections")
    @ApiResponse(responseCode = "200", description = "List of connections")
    public List<DbConnection> index() {
        return connectionsApiController.list();
    }
}
