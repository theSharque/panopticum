package com.panopticum.core.sql;

import lombok.experimental.UtilityClass;

import java.util.function.Function;

@UtilityClass
public class SqlPagingSupport {

    public enum Style {
        LIMIT_OFFSET,
        SQL_SERVER,
        ORACLE
    }

    public boolean hasExplicitSort(String sortBy, String sortOrder) {
        return sortBy != null && !sortBy.isBlank()
                && sortOrder != null && !sortOrder.isBlank()
                && ("asc".equalsIgnoreCase(sortOrder) || "desc".equalsIgnoreCase(sortOrder));
    }

    public String orderByClause(String sortBy, String sortOrder, Function<String, String> quoteColumn, Style style) {
        if (hasExplicitSort(sortBy, sortOrder)) {
            return " ORDER BY " + quoteColumn.apply(sortBy) + " " + sortOrder.toUpperCase();
        }
        if (style == Style.SQL_SERVER) {
            return " ORDER BY (SELECT NULL)";
        }
        return "";
    }

    public String wrapForSubquery(String trimmed, String sortBy, String sortOrder,
                                  Function<String, String> quoteColumn, Style style) {
        if (hasExplicitSort(sortBy, sortOrder)) {
            return "SELECT * FROM (" + trimmed + ") AS _paged"
                    + orderByClause(sortBy, sortOrder, quoteColumn, style);
        }
        return trimmed;
    }

    public String wrapPagedSelect(String sql, int limit, int offset, String sortBy, String sortOrder,
                                  Style style, Function<String, String> quoteColumn) {
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        String upper = trimmed.toUpperCase().stripLeading();
        if (!upper.startsWith("SELECT") || upper.startsWith("SELECT INTO")) {
            return sql;
        }
        int safeOffset = Math.max(0, offset);
        int safeLimit = Math.max(1, limit);

        if (hasExplicitSort(sortBy, sortOrder)) {
            String wrapped = "SELECT * FROM (" + trimmed + ") AS _paged"
                    + orderByClause(sortBy, sortOrder, quoteColumn, style);
            return applyPagination(wrapped, safeLimit, safeOffset, style, true);
        }

        return applyPagination(trimmed, safeLimit, safeOffset, style, false);
    }

    private String applyPagination(String sql, int limit, int offset, Style style, boolean sortedWrap) {
        return switch (style) {
            case LIMIT_OFFSET -> sql + " LIMIT " + limit + " OFFSET " + offset;
            case SQL_SERVER -> {
                if (!sortedWrap && offset == 0) {
                    yield "SELECT TOP (" + limit + ") * FROM (" + sql + ") AS _paged";
                }
                String inner = sortedWrap ? sql : "SELECT * FROM (" + sql + ") AS _paged";
                String orderBy = sortedWrap ? "" : " ORDER BY (SELECT NULL)";
                yield inner + orderBy + " OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
            }
            case ORACLE -> {
                if (!sortedWrap) {
                    if (offset == 0) {
                        yield "SELECT * FROM (" + sql + ") WHERE ROWNUM <= " + limit;
                    }
                    yield "SELECT * FROM (SELECT inner_q.*, ROWNUM rn FROM (" + sql
                            + ") inner_q WHERE ROWNUM <= " + (offset + limit) + ") WHERE rn > " + offset;
                }
                yield sql + " OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
            }
        };
    }
}
