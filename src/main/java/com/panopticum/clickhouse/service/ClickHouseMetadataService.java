package com.panopticum.clickhouse.service;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import io.micronaut.core.annotation.NonNull;
import com.panopticum.core.util.StringUtils;
import com.panopticum.clickhouse.model.ChDatabaseInfo;
import com.panopticum.clickhouse.model.ChTableInfo;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

@Singleton
@Slf4j
public class ClickHouseMetadataService {

    private static final String JDBC_PREFIX = "jdbc:ch:http://";

    private final DbConnectionService dbConnectionService;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    @Value("${panopticum.limits.tables:1000}")
    private int tablesLimit;

    public ClickHouseMetadataService(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    public Optional<Connection> getConnection(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .flatMap(this::createConnection);
    }

    public Optional<Connection> getConnection(Long connectionId, String dbName) {
        return dbConnectionService.findById(connectionId)
                .filter(c -> "clickhouse".equalsIgnoreCase(c.getType()))
                .flatMap(c -> createConnectionToDb(c, dbName != null && !dbName.isBlank() ? dbName : c.getDbName()));
    }

    private Optional<Connection> createConnectionToDb(DbConnection conn, String db) {
        if (db == null || db.isBlank()) {
            db = "default";
        }
        String url = buildUrl(conn.getHost(), conn.getPort(), db);
        try {
            Properties props = new Properties();
            if (conn.getUsername() != null && !conn.getUsername().isBlank()) {
                props.setProperty("user", conn.getUsername());
            }
            if (conn.getPassword() != null) {
                props.setProperty("password", conn.getPassword());
            }
            return Optional.of(DriverManager.getConnection(url, props));
        } catch (SQLException e) {
            log.warn("Failed to connect to {} db {}: {}", conn.getName(), db, e.getMessage());
            return Optional.empty();
        }
    }

    public Optional<String> testConnection(String host, int port, String database, String username, String password) {
        if (host == null || host.isBlank()) {
            return Optional.of("error.specifyHost");
        }
        String db = database != null && !database.isBlank() ? database : "default";
        String url = buildUrl(host.trim(), port, db);
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
        } catch (SQLException e) {
            return Optional.of(e.getMessage());
        }
    }

    private Optional<Connection> createConnection(DbConnection conn) {
        if (!"clickhouse".equalsIgnoreCase(conn.getType())) {
            return Optional.empty();
        }
        String db = conn.getDbName();
        if (db == null || db.isBlank()) {
            db = "default";
        }
        return createConnectionToDb(conn, db);
    }

    private static String buildUrl(String host, int port, String database) {
        return JDBC_PREFIX + host + ":" + port + "/" + database;
    }

    private static final String LIST_DATABASES_SQL =
            "SELECT d.name, coalesce(t.bytes, 0) AS size FROM system.databases d "
            + "LEFT JOIN (SELECT database AS name, sum(total_bytes) AS bytes FROM system.tables GROUP BY database) t ON d.name = t.name ORDER BY d.name";

    public List<ChDatabaseInfo> listDatabaseInfos(Long connectionId) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            List<ChDatabaseInfo> infos = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_DATABASES_SQL);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("name");
                    long size = rs.getLong("size");
                    infos.add(new ChDatabaseInfo(name, size, formatSize(size)));
                }
            }
            return infos;
        } catch (SQLException e) {
            log.warn("listDatabaseInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    private static String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static final String LIST_TABLES_SQL =
            "SELECT name, engine AS type, total_rows, total_bytes FROM system.tables WHERE database = ? ORDER BY name";

    public List<ChTableInfo> listTableInfos(Long connectionId, String dbName) {
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null || dbName == null || dbName.isBlank()) {
                return List.of();
            }
            List<ChTableInfo> tables = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_TABLES_SQL)) {
                ps.setString(1, dbName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString("name");
                        String type = rs.getString("type");
                        long rows = rs.getLong("total_rows");
                        long size = rs.getLong("total_bytes");
                        tables.add(new ChTableInfo(name, type != null ? type : "table", rows, size, formatSize(size)));
                    }
                }
            }
            return tables;
        } catch (SQLException e) {
            log.warn("listTableInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    public Optional<@NonNull QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                                      String sortBy, String sortOrder) {
        return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<@NonNull QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                                       String sortBy, String sortOrder, boolean truncateCells) {
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return Optional.of(QueryResult.error("Connection not available"));
            }
            String pagedSql = wrapWithLimitOffset(sql.trim(), limit, offset, sortBy, sortOrder);
            try (var stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(pagedSql)) {
                var meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                List<String> columns = new ArrayList<>();
                List<String> columnTypes = new ArrayList<>();
                for (int i = 1; i <= colCount; i++) {
                    columns.add(meta.getColumnLabel(i));
                    columnTypes.add(meta.getColumnTypeName(i));
                }
                List<List<Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= colCount; i++) {
                        Object cell = rs.getObject(i);
                        row.add(truncateCells ? StringUtils.truncateCell(cell) : cell);
                    }
                    rows.add(row);
                }

                boolean hasMore = rows.size() == limit;
                QueryResult result = new QueryResult(columns, columnTypes, rows, null, null, offset, limit, hasMore);
                Optional<@NonNull QueryResult> opt = Optional.of(result);
                return opt;
            }
        } catch (SQLException e) {
            log.warn("executeQuery failed: {}", e.getMessage());
            return Optional.of(QueryResult.error(e.getMessage()));
        }
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
