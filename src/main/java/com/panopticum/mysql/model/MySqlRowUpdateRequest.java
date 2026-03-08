package com.panopticum.mysql.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Serdeable
@Schema(description = "MySQL row update request")
public class MySqlRowUpdateRequest {

    @NotBlank
    @Schema(description = "SQL that produced the row", required = true)
    private String sql;

    @NotNull
    @Schema(description = "Row index (0-based)", required = true)
    private Integer rowNum;

    @NotBlank
    @Schema(description = "Qualified table name", required = true)
    private String qualifiedTable;

    @NotEmpty
    @Schema(description = "Primary/unique key column names", required = true)
    private List<String> uniqueKeyColumns;

    @Schema(description = "Sort column")
    private String sort;

    @Schema(description = "Sort order: asc, desc")
    private String order;

    @Schema(description = "Search term")
    private String search;

    @Schema(description = "Column names to values (includes key columns for WHERE)", required = true)
    private Map<String, String> columnValues;
}
