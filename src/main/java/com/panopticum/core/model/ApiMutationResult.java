package com.panopticum.core.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Result of a mutating API action")
public class ApiMutationResult {

    @Schema(description = "Whether the action succeeded")
    private boolean success;

    @Schema(description = "Error message key when success is false")
    private String error;

    public static ApiMutationResult success() {
        return new ApiMutationResult(true, null);
    }

    public static ApiMutationResult failure(String errorKey) {
        return new ApiMutationResult(false, errorKey);
    }
}
