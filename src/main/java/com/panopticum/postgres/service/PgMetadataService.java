package com.panopticum.postgres.service;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.util.StringUtils;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.postgres.model.PgDatabaseInfo;
import com.panopticum.postgres.model.PgSchemaInfo;
import com.panopticum.postgres.model.TableInfo;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Singleton
@Slf4j
public class PgMetadataService {

    private static final String POSTGRESQL_PREFIX = "jdbc:postgresql://";

    private final DbConnectionService dbConnectionService;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    @Value("${panopticum.limits.schemas:500}")
    private int schemasLimit;

    @Value("${panopticum.limits.tables:1000}")
    private int tablesLimit;

    public PgMetadataService(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    public Optional<Connection> getConnection(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .flatMap(this::createConnection);
    }

    public Optional<Connection> getConnection(Long connectionId, String dbName) {
        return dbConnectionService.findById(connectionId)
                .filter(c -> "postgresql".equalsIgnoreCase(c.getType()))
                .flatMap(c -> createConnectionToDb(c, dbName != null && !dbName.isBlank() ? dbName : c.getDbName()));
    }

    private Optional<Connection> createConnectionToDb(DbConnection conn, String db) {
        if (db == null || db.isBlank()) {
            db = "postgres";
        }
        String url = POSTGRESQL_PREFIX + conn.getHost() + ":" + conn.getPort() + "/" + db;
        try {
            return Optional.of(DriverManager.getConnection(url, conn.getUsername(), conn.getPassword() != null ? conn.getPassword() : ""));
        } catch (SQLException e) {
            log.warn("Failed to connect to {} db {}: {}", conn.getName(), db, e.getMessage());
            return Optional.empty();
        }
    }

    public Optional<String> testConnection(String host, int port, String dbName, String username, String password) {
        if (host == null || host.isBlank() || dbName == null || dbName.isBlank() || username == null || username.isBlank()) {
            return Optional.of("Укажите хост, базу и пользователя");
        }

        String url = POSTGRESQL_PREFIX + host.trim() + ":" + port + "/" + dbName.trim();

        try (Connection c = DriverManager.getConnection(url, username.trim(), password != null ? password : "")) {
            return Optional.empty();
        } catch (SQLException e) {
            return Optional.of(e.getMessage());
        }
    }

    private Optional<Connection> createConnection(DbConnection conn) {
        if (!"postgresql".equalsIgnoreCase(conn.getType())) {
            return Optional.empty();
        }

        String db = conn.getDbName();
        if (db == null || db.isBlank()) {
            db = "postgres";
        }
        String url = POSTGRESQL_PREFIX + conn.getHost() + ":" + conn.getPort() + "/" + db;

        try {
            Connection c = DriverManager.getConnection(url, conn.getUsername(), conn.getPassword() != null ? conn.getPassword() : "");

            return Optional.of(c);
        } catch (SQLException e) {
            log.warn("Failed to connect to {}: {}", conn.getName(), e.getMessage());
            return Optional.empty();
        }
    }

    private static final String LIST_DATABASES_SQL =
            "SELECT datname, pg_database_size(datname)::bigint AS size FROM pg_catalog.pg_database WHERE datistemplate = false";

    public List<String> listDatabases(Long connectionId) {
        return listDatabaseInfos(connectionId).stream().map(PgDatabaseInfo::getName).toList();
    }

    public List<PgDatabaseInfo> listDatabaseInfos(Long connectionId) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return List.of();
            }

            List<PgDatabaseInfo> infos = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_DATABASES_SQL);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("datname");
                    long size = rs.getLong("size");
                    infos.add(new PgDatabaseInfo(name, size, formatSize(size)));
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

    private static final String LIST_SCHEMAS_SQL =
            "SELECT s.schema_name, s.schema_owner, (SELECT count(*) FROM information_schema.tables t WHERE t.table_schema = s.schema_name AND t.table_type IN ('BASE TABLE', 'VIEW'))::int AS table_count "
            + "FROM information_schema.schemata s WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog') AND s.schema_name NOT LIKE 'pg_toast%'";

    public List<String> listSchemas(Long connectionId, String dbName, int offset, int limit) {
        return listSchemaInfos(connectionId, dbName).stream().map(PgSchemaInfo::getName).toList();
    }

    public List<PgSchemaInfo> listSchemaInfos(Long connectionId, String dbName) {
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            List<PgSchemaInfo> infos = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_SCHEMAS_SQL);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("schema_name");
                    String owner = rs.getString("schema_owner");
                    int tableCount = rs.getInt("table_count");
                    infos.add(new PgSchemaInfo(name, owner != null ? owner : "", tableCount));
                }
            }
            return infos;
        } catch (SQLException e) {
            log.warn("listSchemaInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    private static final String LIST_TABLES_SQL =
            "SELECT c.relname, CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END AS reltype, "
            + "COALESCE(c.reltuples::bigint, 0) AS row_estimate, COALESCE(pg_total_relation_size(c.oid), 0)::bigint AS size "
            + "FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid "
            + "WHERE n.nspname = ? AND c.relkind IN ('r','v') ORDER BY c.relname";

    public List<TableInfo> listTables(Long connectionId, String dbName, String schema, int offset, int limit) {
        List<TableInfo> all = listTableInfos(connectionId, dbName, schema);
        int end = Math.min(offset + Math.min(limit, tablesLimit - offset), all.size());
        return offset < all.size() ? all.subList(offset, end) : List.of();
    }

    public List<TableInfo> listTableInfos(Long connectionId, String dbName, String schema) {
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null || schema == null || schema.isBlank()) {
                return List.of();
            }
            List<TableInfo> tables = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_TABLES_SQL)) {
                ps.setString(1, schema);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString("relname");
                        String type = rs.getString("reltype");
                        long rowEst = rs.getLong("row_estimate");
                        long size = rs.getLong("size");
                        tables.add(new TableInfo(name, type != null ? type : "table", rowEst, size, formatSize(size)));
                    }
                }
            }
            return tables;
        } catch (SQLException e) {
            log.warn("listTableInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                              String sortBy, String sortOrder) {
        return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
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
                    String typeName = meta.getColumnTypeName(i);
                    int nullable = meta.isNullable(i);
                    String nullability = nullable == ResultSetMetaData.columnNoNulls ? " NOT NULL"
                            : (nullable == ResultSetMetaData.columnNullable ? " NULL" : "");
                    columnTypes.add(typeName + nullability);
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
                return Optional.of(new QueryResult(columns, columnTypes, rows, null, null, offset, limit, hasMore));
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
            String quotedCol = "\"" + sortBy.replace("\"", "\"\"") + "\"";
            orderBy = " ORDER BY " + quotedCol + " " + sortOrder.toUpperCase();
        } else {
            orderBy = " ORDER BY 1 ASC";
        }

        return "SELECT * FROM (" + trimmed + ") AS _paged" + orderBy + " LIMIT " + maxLimit + " OFFSET " + Math.max(0, offset);
    }
}
