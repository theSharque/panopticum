package com.panopticum.core.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
@Serdeable
@Schema(description = "Connection create/update request")
public class ConnectionRequest {

    @NotBlank
    @Schema(description = "Connection type: postgresql, mysql, mssql, oracle, mongodb, redis, clickhouse, cassandra, kafka, rabbitmq, elasticsearch", requiredMode = Schema.RequiredMode.REQUIRED)
    private String type;

    @NotBlank
    @Schema(description = "Connection display name", requiredMode = Schema.RequiredMode.REQUIRED)
    private String name;

    @NotBlank
    @Schema(description = "Host", requiredMode = Schema.RequiredMode.REQUIRED)
    private String host;

    @NotNull
    @Schema(description = "Port")
    private Integer port;

    @Schema(description = "Database name (or keyspace, vhost, etc.)")
    private String database;

    @Schema(description = "Username")
    private String username;

    @Schema(description = "Password")
    private String password;

    @Schema(description = "Connection ID for update (omit for create)")
    private Long id;
}
