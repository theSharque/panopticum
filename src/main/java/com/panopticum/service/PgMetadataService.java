package com.panopticum.service;

import com.panopticum.model.DbConnection;
import jakarta.inject.Singleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Singleton
@Slf4j
public class PgMetadataService {

    private static final String POSTGRESQL_PREFIX = "jdbc:postgresql://";

    private final DbConnectionService dbConnectionService;

    public PgMetadataService(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    public Optional<Connection> getConnection(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .flatMap(this::createConnection);
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

        String url = POSTGRESQL_PREFIX + conn.getHost() + ":" + conn.getPort() + "/" + conn.getDbName();
        try {
            Connection c = DriverManager.getConnection(url, conn.getUsername(), conn.getPassword() != null ? conn.getPassword() : "");
            return Optional.of(c);
        } catch (SQLException e) {
            log.warn("Failed to connect to {}: {}", conn.getName(), e.getMessage());
            return Optional.empty();
        }
    }

    public List<String> listDatabases(Long connectionId) {
        Optional<DbConnection> dbConn = dbConnectionService.findById(connectionId);
        if (dbConn.isEmpty() || !"postgresql".equalsIgnoreCase(dbConn.get().getType())) {
            return List.of();
        }
        String dbNameFromConfig = dbConn.get().getDbName();
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            String catalog = conn.getCatalog();
            String name = catalog != null && !catalog.isBlank() ? catalog : dbNameFromConfig;

            return name != null && !name.isBlank() ? List.of(name) : List.of();
        } catch (SQLException e) {
            log.warn("listDatabases failed: {}", e.getMessage());
            return List.of();
        }
    }

    public List<String> listSchemas(Long connectionId, String dbName) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            DatabaseMetaData meta = conn.getMetaData();
            List<String> schemas = new ArrayList<>();
            try (ResultSet rs = meta.getSchemas(dbName, null)) {
                while (rs.next()) {
                    String schema = rs.getString("TABLE_SCHEM");
                    if (schema != null && !"information_schema".equals(schema)) {
                        schemas.add(schema);
                    }
                }
            }

            return schemas;
        } catch (SQLException e) {
            log.warn("listSchemas failed: {}", e.getMessage());
            return List.of();
        }
    }

    public List<TableInfo> listTables(Long connectionId, String dbName, String schema) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            DatabaseMetaData meta = conn.getMetaData();
            List<TableInfo> tables = new ArrayList<>();
            try (ResultSet rs = meta.getTables(dbName, schema, "%", new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String name = rs.getString("TABLE_NAME");
                    String type = rs.getString("TABLE_TYPE");
                    if (name != null) {
                        tables.add(new TableInfo(name, "VIEW".equals(type) ? "view" : "table"));
                    }
                }
            }

            return tables;
        } catch (SQLException e) {
            log.warn("listTables failed: {}", e.getMessage());
            return List.of();
        }
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String sql) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return Optional.of(QueryResult.error("Connection not available"));
            }
            try (var stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                var meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                List<String> columns = new ArrayList<>();
                for (int i = 1; i <= colCount; i++) {
                    columns.add(meta.getColumnLabel(i));
                }
                List<List<Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= colCount; i++) {
                        row.add(rs.getObject(i));
                    }
                    rows.add(row);
                }

                return Optional.of(new QueryResult(columns, rows, null));
            }
        } catch (SQLException e) {
            log.warn("executeQuery failed: {}", e.getMessage());
            return Optional.of(QueryResult.error(e.getMessage()));
        }
    }

    @Getter
    @AllArgsConstructor
    public static class TableInfo {
        private final String name;
        private final String type;
    }

    @Getter
    public static class QueryResult {
        private final List<String> columns;
        private final List<List<Object>> rows;
        private final String error;

        public QueryResult(List<String> columns, List<List<Object>> rows, String error) {
            this.columns = columns != null ? columns : List.of();
            this.rows = rows != null ? rows : List.of();
            this.error = error;
        }

        public static QueryResult error(String message) {
            return new QueryResult(List.of(), List.of(), message);
        }

        public boolean hasError() {
            return error != null;
        }
    }
}
