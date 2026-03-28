package com.panopticum.cassandra.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Serdeable
@Schema(description = "Cassandra CQL query request")
public class CassandraQueryRequest {

    @NotBlank
    @Schema(description = "CQL statement", requiredMode = Schema.RequiredMode.REQUIRED)
    private String cql;

    @Schema(description = "Offset", defaultValue = "0")
    private int offset = 0;

    @Schema(description = "Limit", defaultValue = "100")
    private int limit = 100;
}
