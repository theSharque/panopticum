package com.panopticum.mysql.repository;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.SizeFormatter;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.TableInfo;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class MySqlMetadataRepository {

    private static final String JDBC_PREFIX = "jdbc:mysql://";

    private static final String LIST_DATABASES_SQL =
            "SELECT s.schema_name, COALESCE(SUM(t.data_length + t.index_length), 0) AS size "
                    + "FROM information_schema.schemata s "
                    + "LEFT JOIN information_schema.tables t ON s.schema_name = t.table_schema "
                    + "WHERE s.schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') "
                    + "GROUP BY s.schema_name ORDER BY s.schema_name";

    private static final String LIST_TABLES_SQL =
            "SELECT table_name, table_type, COALESCE(table_rows, 0) AS table_rows, "
                    + "COALESCE(data_length, 0) + COALESCE(index_length, 0) AS size "
                    + "FROM information_schema.tables "
                    + "WHERE table_schema = ? AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name";

    private static final String COLUMN_TYPES_SQL =
            "SELECT column_name, data_type FROM information_schema.columns "
                    + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";

    private static final String UNIQUE_KEY_COLUMNS_SQL =
            "SELECT index_name, column_name, seq_in_index FROM information_schema.statistics "
                    + "WHERE table_schema = ? AND table_name = ? AND (index_name = 'PRIMARY' OR non_unique = 0) "
                    + "ORDER BY (index_name = 'PRIMARY') DESC, index_name, seq_in_index";

    private final DbConnectionService dbConnectionService;

    public Optional<Connection> getConnection(Long connectionId) {
        return dbConnectionService.findById(connectionId).flatMap(this::createConnection);
    }

    public Optional<Connection> getConnection(Long connectionId, String dbName) {
        return dbConnectionService.findById(connectionId)
                .filter(c -> "mysql".equalsIgnoreCase(c.getType()))
                .flatMap(c -> createConnectionToDb(c, dbName != null && !dbName.isBlank() ? dbName : c.getDbName()));
    }

    private Optional<Connection> createConnectionToDb(DbConnection conn, String db) {
        if (db == null || db.isBlank()) {
            db = "mysql";
        }
        String url = JDBC_PREFIX + conn.getHost() + ":" + conn.getPort() + "/" + db;
        try {
            return Optional.of(DriverManager.getConnection(url, conn.getUsername(), conn.getPassword() != null ? conn.getPassword() : ""));
        } catch (SQLException e) {
            log.warn("Failed to connect to {} db {}: {}", conn.getName(), db, e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<Connection> createConnection(DbConnection conn) {
        if (!"mysql".equalsIgnoreCase(conn.getType())) {
            return Optional.empty();
        }
        String db = conn.getDbName();
        if (db == null || db.isBlank()) {
            db = "mysql";
        }
        return createConnectionToDb(conn, db);
    }

    public List<DatabaseInfo> listDatabaseInfos(Long connectionId) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            List<DatabaseInfo> infos = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_DATABASES_SQL);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("schema_name");
                    long size = rs.getLong("size");
                    infos.add(new DatabaseInfo(name, size, SizeFormatter.formatSize(size)));
                }
            }
            return infos;
        } catch (SQLException e) {
            log.warn("listDatabaseInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    public List<TableInfo> listTableInfos(Long connectionId, String dbName) {
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null || dbName == null || dbName.isBlank()) {
                return List.of();
            }
            List<TableInfo> tables = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_TABLES_SQL)) {
                ps.setString(1, dbName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString("table_name");
                        String type = rs.getString("table_type");
                        long rows = rs.getLong("table_rows");
                        long size = rs.getLong("size");
                        String typeStr = "BASE TABLE".equalsIgnoreCase(type) ? "table" : "view";
                        tables.add(new TableInfo(name, typeStr, rows, size, SizeFormatter.formatSize(size)));
                    }
                }
            }
            return tables;
        } catch (SQLException e) {
            log.warn("listTableInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    public Optional<QueryResultData> executeQuery(Long connectionId, String dbName, String sql) {
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return Optional.empty();
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
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
                        row.add(rs.getObject(i));
                    }
                    rows.add(row);
                }
                return Optional.of(new QueryResultData(columns, columnTypes, rows));
            }
        } catch (SQLException e) {
            log.warn("executeQuery failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public Map<String, String> getColumnTypes(Long connectionId, String dbName, String table) {
        if (dbName == null || dbName.isBlank() || table == null || table.isBlank()) {
            return Map.of();
        }
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return Map.of();
            }
            Map<String, String> types = new LinkedHashMap<>();
            try (PreparedStatement ps = conn.prepareStatement(COLUMN_TYPES_SQL)) {
                ps.setString(1, dbName);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        types.put(rs.getString("column_name"), rs.getString("data_type"));
                    }
                }
            }
            return types;
        } catch (SQLException e) {
            log.warn("getColumnTypes failed: {}", e.getMessage());
            return Map.of();
        }
    }

    public List<String> getUniqueKeyColumns(Long connectionId, String dbName, String table) {
        if (dbName == null || dbName.isBlank() || table == null || table.isBlank()) {
            return List.of();
        }
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            List<String> columns = new ArrayList<>();
            String lastIndex = null;
            try (PreparedStatement ps = conn.prepareStatement(UNIQUE_KEY_COLUMNS_SQL)) {
                ps.setString(1, dbName);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String indexName = rs.getString("index_name");
                        if (lastIndex != null && !lastIndex.equals(indexName)) {
                            break;
                        }
                        lastIndex = indexName;
                        columns.add(rs.getString("column_name"));
                    }
                }
            }
            return columns;
        } catch (SQLException e) {
            log.warn("getUniqueKeyColumns failed: {}", e.getMessage());
            return List.of();
        }
    }

    public Optional<Map<String, Object>> executeQuerySingleRow(Long connectionId, String dbName, String sql) {
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return Optional.empty();
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                if (!rs.next()) {
                    return Optional.empty();
                }
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                return Optional.of(row);
            }
        } catch (SQLException e) {
            log.warn("executeQuerySingleRow failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public Optional<String> executeUpdate(Long connectionId, String dbName, String updateSql, List<Object> params) {
        if (params == null) {
            return Optional.of("Missing params");
        }
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return Optional.of("error.connectionNotAvailable");
            }
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }
                int updated = ps.executeUpdate();
                if (updated == 0) {
                    return Optional.of("Row not found or already changed.");
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            log.warn("executeUpdate failed: {}", e.getMessage());
            return Optional.of(e.getMessage());
        }
    }
}
