package com.panopticum.kubernetes.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KubernetesDeploymentInfo {

    private final String name;
    private final String namespace;
    private final int desiredReplicas;
    private final int readyReplicas;
    private final int availableReplicas;
    private final String image;
}
