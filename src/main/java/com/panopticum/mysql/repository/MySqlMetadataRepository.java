package com.panopticum.mysql.repository;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.SizeFormatter;
import com.panopticum.core.error.ConnectionSupport;
import com.panopticum.core.error.MetadataAccessException;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.TableInfo;
import com.panopticum.mcp.model.ColumnInfo;
import com.panopticum.mcp.model.EntityDescription;
import com.panopticum.mcp.model.ForeignKeyInfo;
import com.panopticum.mcp.model.IndexInfo;
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
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId))) {
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
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public List<TableInfo> listTableInfos(Long connectionId, String dbName) {
        if (dbName == null || dbName.isBlank()) {
            return List.of();
        }
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId, dbName))) {
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
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public Optional<QueryResultData> executeQuery(Long connectionId, String dbName, String sql) {
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId, dbName))) {
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
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public Optional<QueryResultData> executeQuery(Long connectionId, String dbName, String sql, List<Object> params) {
        if (params == null || params.isEmpty()) {
            return executeQuery(connectionId, dbName, sql);
        }
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId, dbName))) {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
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
            }
        } catch (SQLException e) {
            log.warn("executeQuery with params failed: {}", e.getMessage());
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public Map<String, String> getColumnTypes(Long connectionId, String dbName, String table) {
        if (dbName == null || dbName.isBlank() || table == null || table.isBlank()) {
            return Map.of();
        }
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId, dbName))) {
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
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public List<String> getUniqueKeyColumns(Long connectionId, String dbName, String table) {
        if (dbName == null || dbName.isBlank() || table == null || table.isBlank()) {
            return List.of();
        }
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId, dbName))) {
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
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public Optional<Map<String, Object>> executeQuerySingleRow(Long connectionId, String dbName, String sql) {
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId, dbName))) {
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
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public Optional<String> executeUpdate(Long connectionId, String dbName, String updateSql, List<Object> params) {
        if (params == null) {
            return Optional.of("Missing params");
        }
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId, dbName))) {
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
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public Optional<EntityDescription> describeTable(Long connectionId, String dbName, String tableName) {
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId, dbName))) {
            List<ColumnInfo> columns = fetchMysqlColumns(conn, dbName, tableName);
            List<String> pk = columns.stream().filter(ColumnInfo::isPrimaryKey).map(ColumnInfo::getName).toList();
            List<ForeignKeyInfo> fks = fetchMysqlForeignKeys(conn, dbName, tableName);
            List<IndexInfo> indexes = fetchMysqlIndexes(conn, dbName, tableName);
            long rowCount = fetchMysqlRowCount(conn, dbName, tableName);

            return Optional.of(EntityDescription.builder()
                    .entityKind("table")
                    .catalog(dbName)
                    .namespace(null)
                    .entity(tableName)
                    .columns(columns)
                    .primaryKey(pk)
                    .foreignKeys(fks)
                    .indexes(indexes)
                    .approximateRowCount(rowCount)
                    .inferredFromSample(false)
                    .notes(List.of())
                    .build());
        } catch (Exception e) {
            log.warn("describeTable failed for {}.{}: {}", dbName, tableName, e.getMessage());
            return Optional.empty();
        }
    }

    private static List<ColumnInfo> fetchMysqlColumns(Connection conn, String dbName, String tableName) throws SQLException {
        String sql = "SELECT c.column_name, c.data_type, c.is_nullable, c.ordinal_position, "
                + "CASE WHEN c.column_key = 'PRI' THEN true ELSE false END AS is_pk "
                + "FROM information_schema.columns c "
                + "WHERE c.table_schema = ? AND c.table_name = ? ORDER BY c.ordinal_position";
        List<ColumnInfo> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, dbName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(ColumnInfo.builder()
                            .name(rs.getString("column_name"))
                            .type(rs.getString("data_type"))
                            .nullable("YES".equals(rs.getString("is_nullable")))
                            .primaryKey(rs.getBoolean("is_pk"))
                            .position(rs.getInt("ordinal_position"))
                            .build());
                }
            }
        }
        return result;
    }

    private static List<ForeignKeyInfo> fetchMysqlForeignKeys(Connection conn, String dbName, String tableName) throws SQLException {
        String sql = "SELECT kcu.column_name, kcu.referenced_table_name, kcu.referenced_column_name "
                + "FROM information_schema.key_column_usage kcu "
                + "JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema "
                + "WHERE kcu.table_schema = ? AND kcu.table_name = ? AND tc.constraint_type = 'FOREIGN KEY'";
        List<ForeignKeyInfo> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, dbName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String refTable = rs.getString("referenced_table_name");
                    String refCol = rs.getString("referenced_column_name");
                    if (refTable != null) {
                        result.add(ForeignKeyInfo.builder()
                                .columns(List.of(rs.getString("column_name")))
                                .references(Map.of("table", refTable, "columns", List.of(refCol != null ? refCol : "")))
                                .build());
                    }
                }
            }
        }
        return result;
    }

    private static List<IndexInfo> fetchMysqlIndexes(Connection conn, String dbName, String tableName) throws SQLException {
        String sql = "SELECT index_name, non_unique, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS cols "
                + "FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? GROUP BY index_name, non_unique";
        List<IndexInfo> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, dbName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String cols = rs.getString("cols");
                    result.add(IndexInfo.builder()
                            .name(rs.getString("index_name"))
                            .unique(rs.getInt("non_unique") == 0)
                            .columns(cols != null ? List.of(cols.split(",")) : List.of())
                            .build());
                }
            }
        }
        return result;
    }

    private static long fetchMysqlRowCount(Connection conn, String dbName, String tableName) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT table_rows FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, dbName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            return 0L;
        }
    }
}
