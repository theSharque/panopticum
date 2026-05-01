package com.panopticum.kubernetes.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KubernetesEventInfo {

    private final String name;
    private final String namespace;
    private final String reason;
    private final String message;
    private final String objectName;
    private final String objectKind;
    private final int count;
    private final String firstTime;
    private final String lastTime;
    private final String type;
}
