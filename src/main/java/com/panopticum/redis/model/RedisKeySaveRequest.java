package com.panopticum.redis.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.util.Map;

@Data
@Serdeable
@Schema(description = "Redis key save request")
public class RedisKeySaveRequest {

    @NotBlank
    @Schema(description = "Key name", required = true)
    private String key;

    @Schema(description = "Value (for string type)")
    private String value;

    @Schema(description = "Key type: string, hash")
    private String type = "string";

    @Schema(description = "Hash fields (for type=hash)")
    private Map<String, String> fields;
}
