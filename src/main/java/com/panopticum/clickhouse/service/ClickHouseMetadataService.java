package com.panopticum.clickhouse.service;

import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.util.StringUtils;
import com.panopticum.clickhouse.model.ChDatabaseInfo;
import com.panopticum.clickhouse.model.ChQueryResultData;
import com.panopticum.clickhouse.model.ChTableInfo;
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

    public List<ChDatabaseInfo> listDatabaseInfos(Long connectionId) {
        return clickHouseMetadataRepository.listDatabaseInfos(connectionId);
    }

    public Page<ChDatabaseInfo> listDatabasesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<ChDatabaseInfo> all = new ArrayList<>(listDatabaseInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<ChDatabaseInfo> comparator = "size".equals(sortBy)
                ? (desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk()))
                : (desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        List<ChDatabaseInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<ChTableInfo> listTableInfos(Long connectionId, String dbName) {
        return clickHouseMetadataRepository.listTableInfos(connectionId, dbName);
    }

    public Page<ChTableInfo> listTablesPaged(Long connectionId, String dbName, int page, int size, String sort, String order) {
        List<ChTableInfo> all = new ArrayList<>(listTableInfos(connectionId, dbName));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<ChTableInfo> comparator;
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
        List<ChTableInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public Optional<@NonNull QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                                      String sortBy, String sortOrder) {
        return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<@NonNull QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                                       String sortBy, String sortOrder, boolean truncateCells) {
        if (clickHouseMetadataRepository.getConnection(connectionId, dbName).isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        String pagedSql = wrapWithLimitOffset(sql.trim(), limit, offset, sortBy, sortOrder);
        Optional<ChQueryResultData> dataOpt = clickHouseMetadataRepository.executeQuery(connectionId, dbName, pagedSql);
        if (dataOpt.isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        ChQueryResultData data = dataOpt.get();
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

    private String wrapWithLimitOffset(String sql, int limit, int offset, String sortBy, String sortOrder) {
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        String upper = trimmed.toUpperCase().stripLeading();
        if (!upper.startsWith("SELECT") || upper.startsWith("SELECT INTO")) {
            return sql;
        }
        int maxLimit = Math.min(limit, queryRowsLimit);
        String orderBy;
        if (sortBy != null && !sortBy.isBlank() && sortOrder != null && !sortOrder.isBlank()
                && ("asc".equalsIgnoreCase(sortOrder) || "desc".equalsIgnoreCase(sortOrder))) {
            String quotedCol = "`" + sortBy.replace("`", "``") + "`";
            orderBy = " ORDER BY " + quotedCol + " " + sortOrder.toUpperCase();
        } else {
            orderBy = " ORDER BY 1 ASC";
        }
        return "SELECT * FROM (" + trimmed + ") AS _paged" + orderBy + " LIMIT " + maxLimit + " OFFSET " + Math.max(0, offset);
    }
}
