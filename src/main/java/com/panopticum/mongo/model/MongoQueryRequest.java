package com.panopticum.mongo.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Serdeable
@Schema(description = "MongoDB query request")
public class MongoQueryRequest {

    @NotBlank
    @Schema(description = "Database name", requiredMode = Schema.RequiredMode.REQUIRED)
    private String dbName;

    @NotBlank
    @Schema(description = "Collection name", requiredMode = Schema.RequiredMode.REQUIRED)
    private String collection;

    @Schema(description = "MQL query (default {})")
    private String query = "{}";

    @Schema(description = "Offset", defaultValue = "0")
    private Integer offset = 0;

    @Schema(description = "Limit", defaultValue = "100")
    private Integer limit = 100;

    @Schema(description = "Sort field", defaultValue = "_id")
    private String sort = "_id";

    @Schema(description = "Sort order: asc, desc")
    private String order = "asc";
}
