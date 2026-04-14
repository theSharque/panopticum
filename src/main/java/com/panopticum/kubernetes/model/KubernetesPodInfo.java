package com.panopticum.kubernetes.model;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class KubernetesPodInfo {

    String name;
    String namespace;
    String phase;
}
