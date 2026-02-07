package com.panopticum.core.util;

public final class StringUtils {

    private static final int MAX_CELL_LENGTH = 200;
    private static final String ELLIPSIS = "\u2026";

    private StringUtils() {
    }

    public static Object truncateCell(Object value) {
        if (value == null) {
            return null;
        }
        String s = value.toString();
        if (s.length() <= MAX_CELL_LENGTH) {
            return value;
        }
        return s.substring(0, MAX_CELL_LENGTH) + ELLIPSIS;
    }

    public static String truncate(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() <= MAX_CELL_LENGTH) {
            return s;
        }
        return s.substring(0, MAX_CELL_LENGTH) + ELLIPSIS;
    }
}
