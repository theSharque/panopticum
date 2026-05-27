package com.panopticum.core.sql;

import lombok.experimental.UtilityClass;

import java.util.regex.Pattern;

@UtilityClass
public class SqlStatementClassifier {

    private static final Pattern RETURNING = Pattern.compile("\\bRETURNING\\b", Pattern.CASE_INSENSITIVE);

    public static String normalize(String sql) {
        if (sql == null) {
            return "";
        }

        return sql.strip().replaceFirst(";+\\s*$", "");
    }

    public static boolean hasReturning(String sql) {
        return RETURNING.matcher(normalize(sql)).find();
    }

    public static boolean isSelect(String sql) {
        String upper = leadingKeyword(sql);
        return upper.startsWith("SELECT") && !upper.startsWith("SELECT INTO");
    }

    public static boolean isMutation(String sql) {
        if (hasReturning(sql)) {
            return false;
        }

        String upper = leadingKeyword(sql);
        return upper.startsWith("INSERT")
                || upper.startsWith("UPDATE")
                || upper.startsWith("DELETE")
                || upper.startsWith("MERGE")
                || upper.startsWith("TRUNCATE");
    }

    public static SqlStatementKind kindOf(String sql) {
        if (isSelect(sql) || hasReturning(sql)) {
            return SqlStatementKind.SELECT;
        }

        if (isMutation(sql)) {
            return SqlStatementKind.MUTATION;
        }

        return SqlStatementKind.OTHER;
    }

    private static String leadingKeyword(String sql) {
        String trimmed = normalize(sql);
        if (trimmed.isEmpty()) {
            return "";
        }

        String upper = trimmed.toUpperCase();
        if (!upper.startsWith("WITH ")) {
            return upper;
        }

        int depth = 0;
        for (int i = 4; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth = Math.max(0, depth - 1);
            } else if (depth == 0 && Character.isWhitespace(c)) {
                String rest = trimmed.substring(i).stripLeading();
                if (!rest.isEmpty()) {
                    return rest.toUpperCase();
                }
            }
        }

        return upper;
    }
}
