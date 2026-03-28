package com.panopticum.postgres.model;

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
@Schema(description = "PostgreSQL row detail")
public class PgRowDetailResponse {

    @Schema(description = "Error key if any")
    private String error;

    @Schema(description = "Row columns as name/type/value")
    private List<Map<String, String>> detailRows;

    @Schema(description = "CTID of the row")
    private String rowCtid;

    public static PgRowDetailResponse fromCtidRowMap(Map<String, Object> result) {
        String error = (String) result.get("error");
        @SuppressWarnings("unchecked")
        List<Map<String, String>> detailRows = (List<Map<String, String>>) result.get("detailRows");
        String rowCtid = (String) result.get("rowCtid");
        return new PgRowDetailResponse(error, detailRows != null ? detailRows : List.of(),
                rowCtid != null ? rowCtid : "");
    }
}
