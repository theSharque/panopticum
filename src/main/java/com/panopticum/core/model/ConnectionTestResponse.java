package com.panopticum.core.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Connection test result")
public class ConnectionTestResponse {

    @Schema(description = "Whether the connection test succeeded")
    private boolean success;

    @Schema(description = "Error key (e.g. connection.notFound) or success key when success=true")
    private String message;
}
