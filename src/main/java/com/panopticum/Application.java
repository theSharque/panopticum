package com.panopticum;

import io.micronaut.openapi.annotation.OpenAPIInclude;
import io.micronaut.runtime.Micronaut;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;

@OpenAPIInclude(packages = "com.panopticum.api.controller")
@OpenAPIDefinition(
        info = @Info(title = "Panopticum API", version = "1.0", description = "REST API for DB operations (MongoDB, Redis, ClickHouse, PostgreSQL, etc.)"),
        security = @SecurityRequirement(name = "basicAuth")
)
@SecurityScheme(name = "basicAuth", type = SecuritySchemeType.HTTP, scheme = "basic", description = "Basic authentication (admin/admin by default)")
public class Application {

    public static void main(String[] args) {
        Micronaut.run(Application.class, args);
    }
}
