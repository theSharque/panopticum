package com.panopticum.postgres.repository;

import com.panopticum.core.model.DatabaseInfo;
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
public class PgMetadataRepository {

    private static final String POSTGRESQL_PREFIX = "jdbc:postgresql://";

    private static final String LIST_DATABASES_SQL =
            "SELECT datname, pg_database_size(datname)::bigint AS size FROM pg_catalog.pg_database WHERE datistemplate = false";

    private static final String LIST_SCHEMAS_SQL =
            "SELECT s.schema_name, s.schema_owner, (SELECT count(*) FROM information_schema.tables t WHERE t.table_schema = s.schema_name AND t.table_type IN ('BASE TABLE', 'VIEW'))::int AS table_count "
                    + "FROM information_schema.schemata s WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog') AND s.schema_name NOT LIKE 'pg_toast%'";

    private static final String LIST_TABLES_SQL =
            "SELECT c.relname, CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END AS reltype, "
                    + "COALESCE(c.reltuples::bigint, 0) AS row_estimate, COALESCE(pg_total_relation_size(c.oid), 0)::bigint AS size "
                    + "FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid "
                    + "WHERE n.nspname = ? AND c.relkind IN ('r','v') ORDER BY c.relname";

    private static final String COLUMN_TYPES_SQL =
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";

    private final DbConnectionService dbConnectionService;

    public Optional<Connection> getConnection(Long connectionId) {
        return dbConnectionService.findById(connectionId).flatMap(this::createConnection);
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
            return Optional.of(DriverManager.getConnection(url, conn.getUsername(), conn.getPassword() != null ? conn.getPassword() : ""));
        } catch (SQLException e) {
            log.warn("Failed to connect to {}: {}", conn.getName(), e.getMessage());

            return Optional.empty();
        }
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
                    String name = rs.getString("datname");
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

    public List<SchemaInfo> listSchemaInfos(Long connectionId, String dbName) {
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return List.of();
            }

            List<SchemaInfo> infos = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(LIST_SCHEMAS_SQL);
                 ResultSet rs = ps.executeQuery()) {

                    while (rs.next()) {
                    String name = rs.getString("schema_name");
                    String owner = rs.getString("schema_owner");
                    int tableCount = rs.getInt("table_count");
                    infos.add(new SchemaInfo(name, owner != null ? owner : "", tableCount));
                }
            }

            return infos;
        } catch (SQLException e) {
            log.warn("listSchemaInfos failed: {}", e.getMessage());

            return List.of();
        }
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
                        tables.add(new TableInfo(name, type != null ? type : "table", rowEst, size, SizeFormatter.formatSize(size)));
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

    public Optional<QueryResultData> executeQuery(Long connectionId, String dbName, String sql, List<Object> params) {
        if (params == null || params.isEmpty()) {
            return executeQuery(connectionId, dbName, sql);
        }
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
            if (conn == null) {
                return Optional.empty();
            }

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
            return Optional.empty();
        }
    }

    public Map<String, String> getColumnTypes(Long connectionId, String dbName, String schema, String table) {
        if (schema == null || schema.isBlank() || table == null || table.isBlank()) {
            return Map.of();
        }
        try (Connection conn = getConnection(connectionId, dbName).orElse(null)) {
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

    public Optional<String> executeUpdate(Long connectionId, String dbName, String updateSql, List<String> params) {
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
