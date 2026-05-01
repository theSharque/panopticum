package com.panopticum.kubernetes.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KubernetesServiceInfo {

    private final String name;
    private final String namespace;
    private final String type;
    private final String clusterIP;
    private final String ports;
}
