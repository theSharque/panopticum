package com.panopticum.kubernetes.util;

import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@UtilityClass
public class KubernetesNamespaceCsv {

    public static List<String> parse(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        Set<String> seen = new LinkedHashSet<>();
        for (String part : csv.split(",")) {
            String ns = part.trim();
            if (!ns.isEmpty()) {
                seen.add(ns);
            }
        }
        return new ArrayList<>(seen);
    }
}
