package com.panopticum.prometheus.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class PromMetricInfo {

    private final String name;
    private final String job;
}
