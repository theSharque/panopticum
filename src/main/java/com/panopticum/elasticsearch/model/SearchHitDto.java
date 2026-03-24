package com.panopticum.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Data;

import java.util.Map;

@Data
@Serdeable
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchHitDto {

    @JsonProperty("_id")
    private String id;

    @JsonProperty("_source")
    private Map<String, Object> source;

    @JsonProperty("_score")
    private Double score;
}
