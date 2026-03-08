package com.panopticum.api.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "MySQL row detail")
public class MySqlRowDetailResponse {

    @Schema(description = "Error key if any")
    private String error;

    @Schema(description = "Row columns as name/type/value")
    private List<Map<String, String>> detailRows;

    @Schema(description = "Whether row is editable")
    private boolean editable;

    @Schema(description = "Primary/unique key column names")
    private List<String> uniqueKeyColumns;

    @Schema(description = "Qualified table name")
    private String qualifiedTable;
}
