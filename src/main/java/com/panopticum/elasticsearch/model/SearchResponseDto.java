package com.panopticum.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Data;

import java.util.List;

@Data
@Serdeable
public class SearchResponseDto {

    @JsonProperty("hits")
    private HitsDto hits;

    @Data
    @Serdeable
    public static class HitsDto {
        @JsonProperty("total")
        private TotalDto total;

        @JsonProperty("hits")
        private List<SearchHitDto> hits;

        @Data
        @Serdeable
        public static class TotalDto {
            @JsonProperty("value")
            private Long value;
        }
    }
}
