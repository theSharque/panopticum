package com.panopticum.api.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Serdeable
@Schema(description = "MongoDB document replace request")
public class MongoDocumentReplaceRequest {

    @NotBlank
    @Schema(description = "Collection name", required = true)
    private String collection;

    @NotBlank
    @Schema(description = "Document ID", required = true)
    private String docId;

    @NotBlank
    @Schema(description = "JSON document body", required = true)
    private String body;
}
