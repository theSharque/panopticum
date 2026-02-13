package com.panopticum.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Data;

@Data
@Serdeable
public class ElasticsearchIndexInfo {

    private String index;

    @JsonProperty("docs.count")
    private String docsCount;

    @JsonProperty("store.size")
    private String storeSize;

    public long getDocsCountNum() {
        if (docsCount == null || docsCount.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(docsCount.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
