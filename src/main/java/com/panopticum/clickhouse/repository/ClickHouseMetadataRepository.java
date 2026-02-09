package com.panopticum.clickhouse.repository;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.SizeFormatter;
import com.panopticum.clickhouse.model.ChDatabaseInfo;
import com.panopticum.clickhouse.model.ChQueryResultData;
import com.panopticum.clickhouse.model.ChTableInfo;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
                    infos.add(new ChDatabaseInfo(name, size, SizeFormatter.formatSize(size)));
                }
            }
            return infos;
        } catch (SQLException e) {
            log.warn("listDatabaseInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

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
                        tables.add(new ChTableInfo(name, type != null ? type : "table", rows, size, SizeFormatter.formatSize(size)));
                    }
                }
            }
            return tables;
        } catch (SQLException e) {
            log.warn("listTableInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    public Optional<ChQueryResultData> executeQuery(Long connectionId, String dbName, String sql) {
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return Optional.empty();
            }
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
                return Optional.of(new ChQueryResultData(columns, columnTypes, rows));
            }
        } catch (SQLException e) {
            log.warn("executeQuery failed: {}", e.getMessage());
            return Optional.empty();
        }
    }
}
