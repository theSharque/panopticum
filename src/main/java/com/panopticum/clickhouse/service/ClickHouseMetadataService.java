package com.panopticum.clickhouse.service;

import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.util.StringUtils;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.TableInfo;
import com.panopticum.clickhouse.repository.ClickHouseMetadataRepository;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class ClickHouseMetadataService {

    private final ClickHouseMetadataRepository clickHouseMetadataRepository;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    public Optional<Connection> getConnection(Long connectionId) {
        return clickHouseMetadataRepository.getConnection(connectionId);
    }

    public Optional<Connection> getConnection(Long connectionId, String dbName) {
        return clickHouseMetadataRepository.getConnection(connectionId, dbName);
    }

    public Optional<String> testConnection(String host, int port, String database, String username, String password) {
        if (host == null || host.isBlank()) {
            return Optional.of("error.specifyHost");
        }
        String db = database != null && !database.isBlank() ? database : "default";
        String url = ClickHouseMetadataRepository.buildUrl(host.trim(), port, db);
        try {
            Properties props = new Properties();
            if (username != null && !username.isBlank()) {
                props.setProperty("user", username.trim());
            }
            if (password != null) {
                props.setProperty("password", password);
            }
            try (Connection c = DriverManager.getConnection(url, props)) {
                return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public List<DatabaseInfo> listDatabaseInfos(Long connectionId) {
        return clickHouseMetadataRepository.listDatabaseInfos(connectionId);
    }

    public Page<DatabaseInfo> listDatabasesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<DatabaseInfo> all = new ArrayList<>(listDatabaseInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<DatabaseInfo> comparator = "size".equals(sortBy)
                ? (desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk()))
                : (desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        List<DatabaseInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<TableInfo> listTableInfos(Long connectionId, String dbName) {
        return clickHouseMetadataRepository.listTableInfos(connectionId, dbName);
    }

    public Page<TableInfo> listTablesPaged(Long connectionId, String dbName, int page, int size, String sort, String order) {
        List<TableInfo> all = new ArrayList<>(listTableInfos(connectionId, dbName));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<TableInfo> comparator;
        if ("type".equalsIgnoreCase(sortBy)) {
            comparator = desc ? (a, b) -> (b.getType() != null ? b.getType() : "").compareToIgnoreCase(a.getType() != null ? a.getType() : "")
                    : (a, b) -> (a.getType() != null ? a.getType() : "").compareToIgnoreCase(b.getType() != null ? b.getType() : "");
        } else if ("size".equals(sortBy)) {
            comparator = desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk());
        } else if ("rows".equals(sortBy)) {
            comparator = desc ? (a, b) -> Long.compare(b.getApproximateRowCount(), a.getApproximateRowCount()) : (a, b) -> Long.compare(a.getApproximateRowCount(), b.getApproximateRowCount());
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        List<TableInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public Optional<@NonNull QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                                      String sortBy, String sortOrder) {
        return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<@NonNull QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                                       String sortBy, String sortOrder, String search) {
        if (search != null && !search.isBlank()) {
            return executeQueryWithSearch(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, search.trim());
        }
        return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<@NonNull QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                                       String sortBy, String sortOrder, boolean truncateCells) {
        if (clickHouseMetadataRepository.getConnection(connectionId, dbName).isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        String pagedSql = wrapWithLimitOffset(sql.trim(), limit, offset, sortBy, sortOrder);
        Optional<QueryResultData> dataOpt = clickHouseMetadataRepository.executeQuery(connectionId, dbName, pagedSql);
        if (dataOpt.isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        QueryResultData data = dataOpt.get();
        boolean hasMore = data.getRows().size() == limit;
        List<List<Object>> rows = data.getRows();
        if (truncateCells) {
            List<List<Object>> truncated = new ArrayList<>();
            for (List<Object> row : rows) {
                List<Object> t = new ArrayList<>();
                for (Object cell : row) {
                    t.add(StringUtils.truncateCell(cell));
                }
                truncated.add(t);
            }
            rows = truncated;
        }
        return Optional.of(new QueryResult(data.getColumns(), data.getColumnTypes(), rows, null, null, offset, limit, hasMore));
    }

    private Optional<QueryResult> executeQueryWithSearch(Long connectionId, String dbName, String sql, int offset, int limit,
                                                         String sortBy, String sortOrder, String searchTerm) {
        if (clickHouseMetadataRepository.getConnection(connectionId, dbName).isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        String upper = trimmed.toUpperCase().stripLeading();
        if (!upper.startsWith("SELECT") || upper.startsWith("SELECT INTO")) {
            return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
        }
        String innerWithOrder = buildWrappedQueryWithOrder(trimmed, sortBy, sortOrder);
        Optional<QueryResultData> metaOpt = clickHouseMetadataRepository.executeQuery(connectionId, dbName, innerWithOrder + " LIMIT 0");
        if (metaOpt.isEmpty() || metaOpt.get().getColumns() == null || metaOpt.get().getColumns().isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        List<String> columns = metaOpt.get().getColumns();
        String concatExpr = columns.stream()
                .map(c -> "toString(`_sub`.`" + c.replace("`", "``") + "`)")
                .reduce((a, b) -> "concat(" + a + ", ':', " + b + ")")
                .orElse("''");
        String orderByClause = buildOrderBy(sortBy, sortOrder);
        String searchSql = "SELECT * FROM (" + innerWithOrder + ") AS _sub WHERE " + concatExpr + " LIKE ? " + orderByClause + " LIMIT ? OFFSET ?";
        String likePattern = "%" + escapeForLike(searchTerm) + "%";
        int maxLimit = Math.min(limit, queryRowsLimit);
        List<Object> params = List.of(likePattern, maxLimit, Math.max(0, offset));
        Optional<QueryResultData> dataOpt = clickHouseMetadataRepository.executeQuery(connectionId, dbName, searchSql, params);
        if (dataOpt.isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        QueryResultData data = dataOpt.get();
        boolean hasMore = data.getRows().size() == limit;
        List<List<Object>> rows = data.getRows().size() > limit ? data.getRows().subList(0, limit) : data.getRows();
        List<List<Object>> truncated = new ArrayList<>();
        for (List<Object> row : rows) {
            List<Object> t = new ArrayList<>();
            for (Object cell : row) {
                t.add(StringUtils.truncateCell(cell));
            }
            truncated.add(t);
        }
        return Optional.of(new QueryResult(data.getColumns(), data.getColumnTypes(), truncated, null, null, offset, limit, hasMore));
    }

    private static String escapeForLike(String term) {
        if (term == null) {
            return "";
        }
        return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    private String buildWrappedQueryWithOrder(String trimmed, String sortBy, String sortOrder) {
        return "SELECT * FROM (" + trimmed + ") AS _paged" + buildOrderBy(sortBy, sortOrder);
    }

    private static String buildOrderBy(String sortBy, String sortOrder) {
        if (sortBy != null && !sortBy.isBlank() && sortOrder != null && !sortOrder.isBlank()
                && ("asc".equalsIgnoreCase(sortOrder) || "desc".equalsIgnoreCase(sortOrder))) {
            String quotedCol = "`" + sortBy.replace("`", "``") + "`";
            return " ORDER BY " + quotedCol + " " + sortOrder.toUpperCase();
        }
        return " ORDER BY 1 ASC";
    }

    private String wrapWithLimitOffset(String sql, int limit, int offset, String sortBy, String sortOrder) {
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        String upper = trimmed.toUpperCase().stripLeading();
        if (!upper.startsWith("SELECT") || upper.startsWith("SELECT INTO")) {
            return sql;
        }
        int maxLimit = Math.min(limit, queryRowsLimit);
        return buildWrappedQueryWithOrder(trimmed, sortBy, sortOrder) + " LIMIT " + maxLimit + " OFFSET " + Math.max(0, offset);
    }
}
