package com.panopticum.kubernetes.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class KubernetesIngressInfo {

    private final String name;
    private final String namespace;
    private final List<String> hosts;
    private final String className;
}
