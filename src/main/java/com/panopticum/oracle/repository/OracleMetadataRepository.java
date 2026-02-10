package com.panopticum.oracle.repository;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.SizeFormatter;
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
public class OracleMetadataRepository {

    private static final String LIST_SCHEMAS_SQL =
            "SELECT owner, COUNT(*) AS table_count FROM all_objects "
                    + "WHERE object_type IN ('TABLE','VIEW') GROUP BY owner ORDER BY owner";

    private static final String LIST_TABLES_SQL =
            "SELECT object_name, object_type FROM ("
                    + "SELECT table_name AS object_name, 'TABLE' AS object_type FROM all_tables WHERE owner = ? "
                    + "UNION ALL "
                    + "SELECT view_name AS object_name, 'VIEW' AS object_type FROM all_views WHERE owner = ?"
                    + ") ORDER BY 1";

    private static final String TABLE_SIZE_SQL =
            "SELECT segment_name, NVL(bytes, 0) AS bytes FROM all_segments "
                    + "WHERE owner = ? AND segment_type = 'TABLE'";

    private static final String COLUMN_TYPES_SQL =
            "SELECT column_name, data_type FROM all_tab_columns "
                    + "WHERE owner = ? AND table_name = ? ORDER BY column_id";

    private final DbConnectionService dbConnectionService;

    private static String buildJdbcUrl(DbConnection conn) {
        String host = conn.getHost() != null && !conn.getHost().isBlank() ? conn.getHost() : "localhost";
        int port = conn.getPort() > 0 ? conn.getPort() : 1521;
        String service = conn.getDbName() != null && !conn.getDbName().isBlank() ? conn.getDbName() : "XEPDB1";
        return "jdbc:oracle:thin:@//" + host + ":" + port + "/" + service;
    }

    public Optional<Connection> getConnection(Long connectionId) {
        return dbConnectionService.findById(connectionId).flatMap(this::createConnection);
    }

    public Optional<Connection> getConnection(Long connectionId, String schema) {
        return dbConnectionService.findById(connectionId)
                .filter(c -> "oracle".equalsIgnoreCase(c.getType()))
                .flatMap(c -> createConnection(c, schema));
    }

    private Optional<Connection> createConnection(DbConnection conn, String schema) {
        try {
            Connection c = DriverManager.getConnection(buildJdbcUrl(conn),
                    conn.getUsername(), conn.getPassword() != null ? conn.getPassword() : "");
            if (schema != null && !schema.isBlank()) {
                String userSchema = conn.getUsername() != null ? conn.getUsername().toUpperCase() : "";
                if (!schema.toUpperCase().equals(userSchema)) {
                    try (Statement stmt = c.createStatement()) {
                        String schemaVal = schema.toUpperCase().replace("'", "''").replace("\"", "\"\"");
                        stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = '" + schemaVal + "'");
                    }
                }
            }
            return Optional.of(c);
        } catch (SQLException e) {
            log.warn("Failed to connect to {} schema {}: {}", conn.getName(), schema, e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<Connection> createConnection(DbConnection conn) {
        if (!"oracle".equalsIgnoreCase(conn.getType())) {
            return Optional.empty();
        }
        return createConnection(conn, null);
    }

    public List<SchemaInfo> listSchemaInfos(Long connectionId) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            List<SchemaInfo> infos = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_SCHEMAS_SQL);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("owner");
                    int tableCount = rs.getInt("table_count");
                    infos.add(new SchemaInfo(name, name, tableCount));
                }
            }
            return infos;
        } catch (SQLException e) {
            log.warn("listSchemaInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    public List<TableInfo> listTableInfos(Long connectionId, String schema) {
        try (Connection conn = getConnection(connectionId, schema).orElse(null)) {
            if (conn == null || schema == null || schema.isBlank()) {
                return List.of();
            }
            List<TableInfo> tables = new ArrayList<>();
            Map<String, Long> sizeByTable = loadTableSizes(conn, schema);
            try (PreparedStatement ps = conn.prepareStatement(LIST_TABLES_SQL)) {
                ps.setString(1, schema);
                ps.setString(2, schema);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString("object_name");
                        String type = rs.getString("object_type");
                        long rows = countRows(conn, schema, name);
                        long size = sizeByTable.getOrDefault(name, 0L);
                        String typeStr = "TABLE".equalsIgnoreCase(type) ? "table" : "view";
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

    private Map<String, Long> loadTableSizes(Connection conn, String schema) {
        Map<String, Long> sizes = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(TABLE_SIZE_SQL)) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sizes.put(rs.getString("segment_name"), rs.getLong("bytes"));
                }
            }
        } catch (SQLException e) {
            log.warn("loadTableSizes failed: {}", e.getMessage());
        }
        return sizes;
    }

    private long countRows(Connection conn, String schema, String tableName) {
        String quotedSchema = "\"" + schema.replace("\"", "\"\"") + "\"";
        String quotedTable = "\"" + tableName.replace("\"", "\"\"") + "\"";
        String sql = "SELECT COUNT(*) FROM " + quotedSchema + "." + quotedTable;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            log.debug("countRows failed for {}.{}: {}", schema, tableName, e.getMessage());
            return 0;
        }
    }

    public Optional<QueryResultData> executeQuery(Long connectionId, String schema, String sql) {
        try (Connection conn = getConnection(connectionId, schema).orElse(null)) {
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
            throw new RuntimeException("Query failed: " + e.getMessage(), e);
        }
    }

    public Map<String, String> getColumnTypes(Long connectionId, String schema, String table) {
        if (schema == null || schema.isBlank() || table == null || table.isBlank()) {
            return Map.of();
        }
        try (Connection conn = getConnection(connectionId, schema).orElse(null)) {
            if (conn == null) {
                return Map.of();
            }
            Map<String, String> types = new LinkedHashMap<>();
            try (PreparedStatement ps = conn.prepareStatement(COLUMN_TYPES_SQL)) {
                ps.setString(1, schema);
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

    public Optional<Map<String, Object>> executeQuerySingleRow(Long connectionId, String schema, String sql) {
        try (Connection conn = getConnection(connectionId, schema).orElse(null)) {
            if (conn == null) {
                return Optional.empty();
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                if (!rs.next()) {
                    return Optional.of(new LinkedHashMap<>());
                }
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                return Optional.of(row);
            }
        } catch (SQLException e) {
            log.warn("executeQuerySingleRow failed: {}", e.getMessage());
            throw new RuntimeException("Query failed: " + e.getMessage(), e);
        }
    }

    public Optional<String> executeUpdate(Long connectionId, String schema, String updateSql, List<String> params) {
        if (params == null) {
            return Optional.of("Missing params");
        }
        try (Connection conn = getConnection(connectionId, schema).orElse(null)) {
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
