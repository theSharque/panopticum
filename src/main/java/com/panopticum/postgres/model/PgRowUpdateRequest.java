package com.panopticum.postgres.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.Map;

@Data
@Serdeable
@Schema(description = "PostgreSQL row update request")
public class PgRowUpdateRequest {

    @NotBlank
    @Schema(description = "SQL that produced the row", requiredMode = Schema.RequiredMode.REQUIRED)
    private String sql;

    @NotNull
    @Schema(description = "Row index (0-based)", requiredMode = Schema.RequiredMode.REQUIRED)
    private Integer rowNum;

    @NotBlank
    @Schema(description = "CTID of the row", requiredMode = Schema.RequiredMode.REQUIRED)
    private String ctid;

    @Schema(description = "Sort column")
    private String sort;

    @Schema(description = "Sort order: asc, desc")
    private String order;

    @Schema(description = "Search term")
    private String search;

    @Schema(description = "Column names to values for update", requiredMode = Schema.RequiredMode.REQUIRED)
    private Map<String, String> columnValues;
}
