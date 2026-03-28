package com.panopticum.oracle.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.Map;

@Data
@Serdeable
@Schema(description = "Oracle row update request (by ROWID)")
public class OracleRowUpdateRequest {

    @NotBlank
    @Schema(description = "SQL that produced the row", requiredMode = Schema.RequiredMode.REQUIRED)
    private String sql;

    @NotNull
    @Schema(description = "Row index (0-based)", requiredMode = Schema.RequiredMode.REQUIRED)
    private Integer rowNum;

    @NotBlank
    @Schema(description = "ROWID of the row", requiredMode = Schema.RequiredMode.REQUIRED)
    private String rowid;

    @Schema(description = "Sort column")
    private String sort;

    @Schema(description = "Sort order: asc, desc")
    private String order;

    @Schema(description = "Column names to values for update", requiredMode = Schema.RequiredMode.REQUIRED)
    private Map<String, String> columnValues;
}
