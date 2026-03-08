package com.panopticum.api.controller;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.MediaType;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;

import java.util.List;

@Controller("/api")
@Secured(SecurityRule.IS_AUTHENTICATED)
@RequiredArgsConstructor
@Tag(name = "Home", description = "API root")
public class HomeApiController {

    private final DbConnectionService dbConnectionService;

    @Get
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List all connections")
    @ApiResponse(responseCode = "200", description = "List of connections")
    public List<DbConnection> index() {
        return dbConnectionService.findAll();
    }
}
