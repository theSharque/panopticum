package com.panopticum.oracle.model;

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
@Schema(description = "Oracle row detail")
public class OracleRowDetailResponse {

    @Schema(description = "Error key if any")
    private String error;

    @Schema(description = "Row columns as name/type/value")
    private List<Map<String, String>> detailRows;

    @Schema(description = "ROWID of the row")
    private String rowRowid;

    public static OracleRowDetailResponse fromRowidRowMap(Map<String, Object> result) {
        String error = (String) result.get("error");
        @SuppressWarnings("unchecked")
        List<Map<String, String>> detailRows = (List<Map<String, String>>) result.get("detailRows");
        String rowRowid = (String) result.get("rowRowid");
        return new OracleRowDetailResponse(error, detailRows != null ? detailRows : List.of(),
                rowRowid != null ? rowRowid : "");
    }
}
