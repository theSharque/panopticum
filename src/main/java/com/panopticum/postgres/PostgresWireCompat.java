package com.panopticum.postgres;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PostgresWireCompat {

    public static boolean isCompatible(String type) {
        if (type == null || type.isBlank()) {
            return false;
        }
        return switch (type.toLowerCase()) {
            case "postgresql", "greenplum", "yugabytedb", "cockroachdb" -> true;
            default -> false;
        };
    }

    public static boolean supportsPgStatsAndSizes(String type) {
        if (type == null || type.isBlank()) {
            return false;
        }
        String t = type.toLowerCase();
        return "postgresql".equals(t) || "greenplum".equals(t);
    }

    public static boolean supportsCtidUpdates(String type) {
        if (type == null || type.isBlank()) {
            return false;
        }
        String t = type.toLowerCase();
        return "postgresql".equals(t) || "greenplum".equals(t);
    }
}
