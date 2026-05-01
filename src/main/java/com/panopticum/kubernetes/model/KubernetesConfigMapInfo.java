package com.panopticum.kubernetes.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class KubernetesConfigMapInfo {

    private final String name;
    private final String namespace;
    private final List<String> keys;
}
