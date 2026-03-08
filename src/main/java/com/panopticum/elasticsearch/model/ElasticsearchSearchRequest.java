package com.panopticum.elasticsearch.model;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
@Serdeable
@Schema(description = "Elasticsearch search request")
public class ElasticsearchSearchRequest {

    @Schema(description = "Query DSL JSON", example = "{\"query\":{\"match_all\":{}}}")
    private String query = "{\"query\":{\"match_all\":{}}}";

    @Schema(description = "Offset", defaultValue = "0")
    private int offset = 0;

    @Schema(description = "Limit", defaultValue = "100")
    private int limit = 100;
}
