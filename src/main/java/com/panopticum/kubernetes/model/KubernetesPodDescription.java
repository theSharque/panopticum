package com.panopticum.kubernetes.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class KubernetesPodDescription {

    private final String name;
    private final String namespace;
    private final String phase;
    private final String nodeName;
    private final String podIP;
    private final List<ContainerInfo> containers;
    private final List<Map<String, Object>> conditions;
    private final List<KubernetesEventInfo> recentEvents;

    @Data
    @Builder
    public static class ContainerInfo {
        private final String name;
        private final String image;
        private final boolean ready;
        private final int restartCount;
    }
}
