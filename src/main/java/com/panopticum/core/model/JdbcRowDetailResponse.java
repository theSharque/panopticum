package com.panopticum.core.model;

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
@Schema(description = "JDBC row detail (MySQL, SQL Server, similar)")
public class JdbcRowDetailResponse {

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

    public static JdbcRowDetailResponse fromEditableRowMap(Map<String, Object> result) {
        String error = (String) result.get("error");
        @SuppressWarnings("unchecked")
        List<Map<String, String>> detailRows = (List<Map<String, String>>) result.get("detailRows");
        boolean editable = Boolean.TRUE.equals(result.get("editable"));
        @SuppressWarnings("unchecked")
        List<String> uniqueKeyColumns = (List<String>) result.get("uniqueKeyColumns");
        String qualifiedTable = (String) result.get("qualifiedTable");
        return new JdbcRowDetailResponse(error, detailRows != null ? detailRows : List.of(),
                editable, uniqueKeyColumns != null ? uniqueKeyColumns : List.of(),
                qualifiedTable != null ? qualifiedTable : "");
    }
}
