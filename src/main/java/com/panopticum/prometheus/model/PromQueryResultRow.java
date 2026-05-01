package com.panopticum.prometheus.model;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@Getter
@Builder
public class PromQueryResultRow {

    private final String timestamp;
    private final String value;
    private final Map<String, String> labels;
}
