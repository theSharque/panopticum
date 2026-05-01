package com.panopticum.clickhouse.repository;

import com.panopticum.core.error.ConnectionSupport;
import com.panopticum.core.error.MetadataAccessException;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.SizeFormatter;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.TableInfo;
import com.panopticum.mcp.model.ColumnInfo;
import com.panopticum.mcp.model.EntityDescription;
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
import java.util.List;
import java.util.Optional;
import java.util.Properties;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class ClickHouseMetadataRepository {

    private static final String JDBC_PREFIX = "jdbc:ch:http://";

    private static final String LIST_DATABASES_SQL =
            "SELECT d.name, coalesce(t.bytes, 0) AS size FROM system.databases d "
                    + "LEFT JOIN (SELECT database AS name, sum(total_bytes) AS bytes FROM system.tables GROUP BY database) t ON d.name = t.name ORDER BY d.name";

    private static final String LIST_TABLES_SQL =
            "SELECT name, engine AS type, total_rows, total_bytes FROM system.tables WHERE database = ? ORDER BY name";

    private final DbConnectionService dbConnectionService;

    public Optional<Connection> getConnection(Long connectionId) {
        return dbConnectionService.findById(connectionId).flatMap(this::createConnection);
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

    public static String buildUrl(String host, int port, String database) {
        return JDBC_PREFIX + host + ":" + port + "/" + database;
    }

    public List<DatabaseInfo> listDatabaseInfos(Long connectionId) {
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId))) {
            List<DatabaseInfo> infos = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_DATABASES_SQL);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("name");
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
                        String name = rs.getString("name");
                        String type = rs.getString("type");
                        long rows = rs.getLong("total_rows");
                        long size = rs.getLong("total_bytes");
                        tables.add(new TableInfo(name, type != null ? type : "table", rows, size, SizeFormatter.formatSize(size)));
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
                        columnTypes.add(meta.getColumnTypeName(i));
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

    public Optional<EntityDescription> describeTable(Long connectionId, String dbName, String tableName) {
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId, dbName))) {
            List<ColumnInfo> columns = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT name, type, is_in_primary_key, position FROM system.columns WHERE database = ? AND table = ? ORDER BY position")) {
                ps.setString(1, dbName);
                ps.setString(2, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        columns.add(ColumnInfo.builder()
                                .name(rs.getString("name"))
                                .type(rs.getString("type"))
                                .nullable(rs.getString("type") != null && rs.getString("type").startsWith("Nullable"))
                                .primaryKey(rs.getInt("is_in_primary_key") == 1)
                                .position(rs.getInt("position"))
                                .build());
                    }
                }
            }

            List<String> pk = columns.stream().filter(ColumnInfo::isPrimaryKey).map(ColumnInfo::getName).toList();
            long rowCount = 0;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT total_rows FROM system.tables WHERE database = ? AND name = ?")) {
                ps.setString(1, dbName);
                ps.setString(2, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) rowCount = rs.getLong(1);
                }
            }

            return Optional.of(EntityDescription.builder()
                    .entityKind("table")
                    .catalog(dbName)
                    .namespace(null)
                    .entity(tableName)
                    .columns(columns)
                    .primaryKey(pk)
                    .foreignKeys(List.of())
                    .indexes(List.of())
                    .approximateRowCount(rowCount)
                    .inferredFromSample(false)
                    .notes(List.of())
                    .build());
        } catch (Exception e) {
            log.warn("describeTable failed for {}.{}: {}", dbName, tableName, e.getMessage());
            return Optional.empty();
        }
    }
}
