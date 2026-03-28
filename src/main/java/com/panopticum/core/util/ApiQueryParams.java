package com.panopticum.core.util;

public final class ApiQueryParams {

    private ApiQueryParams() {
    }

    public static int normalizedOffset(Integer offset) {
        return offset != null ? Math.max(0, offset) : 0;
    }

    public static int normalizedLimit(Integer limit) {
        return limit != null && limit > 0 ? Math.min(limit, 1000) : 100;
    }

    public static int normalizedOffset(int offset) {
        return Math.max(0, offset);
    }

    public static int normalizedLimit(int limit) {
        return limit > 0 ? Math.min(limit, 1000) : 100;
    }

    public static String trimmedSearchOrEmpty(String search) {
        return search != null && !search.isBlank() ? search.trim() : "";
    }
}
