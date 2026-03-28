package com.panopticum.mongo.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Serdeable
@Schema(description = "MongoDB document replace request")
public class MongoDocumentReplaceRequest {

    @NotBlank
    @Schema(description = "Collection name", requiredMode = Schema.RequiredMode.REQUIRED)
    private String collection;

    @NotBlank
    @Schema(description = "Document ID", requiredMode = Schema.RequiredMode.REQUIRED)
    private String docId;

    @NotBlank
    @Schema(description = "JSON document body", requiredMode = Schema.RequiredMode.REQUIRED)
    private String body;
}
