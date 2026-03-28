package com.panopticum.core.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Serdeable
@Schema(description = "SQL query request")
public class SqlQueryRequest {

    @NotBlank
    @Schema(description = "SQL query", requiredMode = Schema.RequiredMode.REQUIRED)
    private String sql;

    @NotBlank
    @Schema(description = "Database name", requiredMode = Schema.RequiredMode.REQUIRED)
    private String dbName;

    @Schema(description = "Schema name")
    private String schemaName;

    @Schema(description = "Offset", defaultValue = "0")
    private Integer offset = 0;

    @Schema(description = "Limit", defaultValue = "100")
    private Integer limit = 100;

    @Schema(description = "Sort column")
    private String sort;

    @Schema(description = "Sort order: asc, desc")
    private String order;

    @Schema(description = "Search term for filtering rows")
    private String search;
}
