package com.panopticum.core.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Serdeable
@Schema(description = "Connection test request")
public class ConnectionTestRequest {

    @NotBlank
    @Schema(description = "Connection type: postgresql, mysql, mssql, oracle, mongodb, redis, clickhouse, cassandra, kafka, rabbitmq, elasticsearch", required = true)
    private String type;

    @Schema(description = "Host")
    private String host;

    @Schema(description = "Port")
    private Integer port;

    @Schema(description = "Database name")
    private String database;

    @Schema(description = "Username")
    private String username;

    @Schema(description = "Password (optional if id provided)")
    private String password;

    @Schema(description = "Existing connection ID to use credentials from")
    private Long id;
}
