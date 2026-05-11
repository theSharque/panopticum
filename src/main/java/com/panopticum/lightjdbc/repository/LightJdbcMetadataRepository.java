package com.panopticum.lightjdbc.repository;

import com.panopticum.core.error.ConnectionSupport;
import com.panopticum.core.error.MetadataAccessException;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.mcp.model.ColumnInfo;
import com.panopticum.mcp.model.EntityDescription;
import com.panopticum.mcp.model.ForeignKeyInfo;
import com.panopticum.mcp.model.IndexInfo;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class LightJdbcMetadataRepository {

    private final DbConnectionService dbConnectionService;

    public static boolean isLightJdbcType(String type) {
        if (type == null || type.isBlank()) {
            return false;
        }
        return switch (type.toLowerCase(Locale.ROOT)) {
            case "h2", "hsqldb", "derby" -> true;
            default -> false;
        };
    }

    public Optional<Connection> getConnection(Long connectionId) {
        Optional<DbConnection> conn = dbConnectionService.findById(connectionId);
        if (conn.isEmpty() || !isLightJdbcType(conn.get().getType())) {
            return Optional.empty();
        }
        try {
            DbConnection c = conn.get();
            return Optional.of(DriverManager.getConnection(buildUrl(c), nullToEmpty(c.getUsername()),
                    c.getPassword() != null ? c.getPassword() : ""));
        } catch (SQLException e) {
            log.warn("Light JDBC connect failed id={}: {}", connectionId, e.getMessage());
            return Optional.empty();
        }
    }

    public static String buildUrl(DbConnection c) {
        String host = c.getHost() != null ? c.getHost().trim() : "localhost";
        int port = c.getPort() > 0 ? c.getPort() : defaultPort(c.getType());
        String db = c.getDbName() != null ? c.getDbName().trim() : "";
        String t = c.getType() != null ? c.getType().toLowerCase(Locale.ROOT) : "";
        return switch (t) {
            case "h2" -> "jdbc:h2:tcp://" + host + ":" + port + "/" + db;
            case "hsqldb" -> "jdbc:hsqldb:hsql://" + host + ":" + port + "/" + db;
            case "derby" -> "jdbc:derby://" + host + ":" + port + "/" + db + ";create=false";
            default -> throw new IllegalArgumentException("unsupported light jdbc type: " + t);
        };
    }

    public static int defaultPort(String type) {
        if (type == null) {
            return 9092;
        }
        return switch (type.toLowerCase(Locale.ROOT)) {
            case "h2" -> 9092;
            case "hsqldb" -> 9001;
            case "derby" -> 1527;
            default -> 9092;
        };
    }

    public List<SchemaInfo> listSchemaInfos(Long connectionId) {
        DbConnection cfg = dbConnectionService.findById(connectionId).orElse(null);
        if (cfg == null || !isLightJdbcType(cfg.getType())) {
            return List.of();
        }
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId))) {
            Set<String> names = new LinkedHashSet<>();
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getSchemas()) {
                while (rs.next()) {
                    String schema = rs.getString("TABLE_SCHEM");
                    if (schema != null && !isSystemSchema(cfg.getType(), schema)) {
                        names.add(schema);
                    }
                }
            }
            if (names.isEmpty()) {
                try (ResultSet rs = md.getSchemas(conn.getCatalog(), null)) {
                    while (rs.next()) {
                        String schema = rs.getString("TABLE_SCHEM");
                        if (schema != null && !isSystemSchema(cfg.getType(), schema)) {
                            names.add(schema);
                        }
                    }
                }
            }
            List<SchemaInfo> out = new ArrayList<>();
            for (String n : names) {
                out.add(new SchemaInfo(n, "", 0));
            }
            out.sort(Comparator.comparing(SchemaInfo::getName, String.CASE_INSENSITIVE_ORDER));
            return out;
        } catch (SQLException e) {
            log.warn("listSchemaInfos failed: {}", e.getMessage());
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public List<TableInfo> listTableInfos(Long connectionId, String schema) {
        if (schema == null || schema.isBlank()) {
            return List.of();
        }
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId))) {
            DatabaseMetaData md = conn.getMetaData();
            List<TableInfo> tables = new ArrayList<>();
            try (ResultSet rs = md.getTables(conn.getCatalog(), schema, "%", new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String name = rs.getString("TABLE_NAME");
                    String typ = rs.getString("TABLE_TYPE");
                    if (name == null || name.isBlank()) {
                        continue;
                    }
                    String typeStr = typ != null ? typ : "TABLE";
                    tables.add(new TableInfo(name, typeStr, 0L, 0L, ""));
                }
            }
            tables.sort(Comparator.comparing(TableInfo::getName, String.CASE_INSENSITIVE_ORDER));
            return tables;
        } catch (SQLException e) {
            log.warn("listTableInfos failed: {}", e.getMessage());
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public Optional<QueryResultData> executeQuery(Long connectionId, String sql) {
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId))) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                return Optional.of(readResultSet(rs));
            }
        } catch (SQLException e) {
            log.warn("executeQuery failed: {}", e.getMessage());
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public Optional<QueryResultData> executeQueryWindow(Long connectionId, String sql, int offset, int limit) {
        int safeLimit = Math.max(1, limit);
        int safeOffset = Math.max(0, offset);
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId))) {
            try (Statement stmt = conn.createStatement()) {
                stmt.setMaxRows(safeOffset + safeLimit);
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    for (int i = 0; i < safeOffset && rs.next(); i++) {
                    }
                    QueryResultData chunk = readResultSetLimited(rs, safeLimit);
                    return Optional.of(chunk);
                }
            }
        } catch (SQLException e) {
            log.warn("executeQueryWindow failed: {}", e.getMessage());
            throw new MetadataAccessException(e.getMessage(), e);
        }
    }

    public Optional<Map<String, Object>> executeQuerySingleRow(Long connectionId, String sql) {
        Optional<QueryResultData> data = executeQueryWindow(connectionId, sql, 0, 1);
        if (data.isEmpty() || data.get().getRows().isEmpty()) {
            return Optional.empty();
        }
        List<String> cols = data.get().getColumns();
        List<Object> row = data.get().getRows().get(0);
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < cols.size() && i < row.size(); i++) {
            map.put(cols.get(i), row.get(i));
        }
        return Optional.of(map);
    }

    public Optional<EntityDescription> describeTable(Long connectionId, String schema, String table) {
        if (schema == null || schema.isBlank() || table == null || table.isBlank()) {
            return Optional.empty();
        }
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId))) {
            DatabaseMetaData md = conn.getMetaData();
            String catalog = conn.getCatalog();
            List<ColumnInfo> columns = new ArrayList<>();
            try (ResultSet rs = md.getColumns(catalog, schema, table, "%")) {
                while (rs.next()) {
                    String col = rs.getString("COLUMN_NAME");
                    String type = rs.getString("TYPE_NAME");
                    int pos = rs.getInt("ORDINAL_POSITION");
                    String isNull = rs.getString("IS_NULLABLE");
                    boolean nullable = isNull != null && isNull.equalsIgnoreCase("YES");
                    columns.add(ColumnInfo.builder()
                            .name(col)
                            .type(type != null ? type : "")
                            .nullable(nullable)
                            .primaryKey(false)
                            .position(pos)
                            .build());
                }
            }
            List<String> pk = new ArrayList<>();
            try (ResultSet rs = md.getPrimaryKeys(catalog, schema, table)) {
                while (rs.next()) {
                    pk.add(rs.getString("COLUMN_NAME"));
                }
            }
            List<ColumnInfo> withPk = new ArrayList<>();
            Set<String> pkSet = Set.copyOf(pk);
            for (ColumnInfo c : columns) {
                withPk.add(ColumnInfo.builder()
                        .name(c.getName())
                        .type(c.getType())
                        .nullable(c.isNullable())
                        .primaryKey(pkSet.contains(c.getName()))
                        .position(c.getPosition())
                        .build());
            }
            List<IndexInfo> indexes = listIndexes(md, catalog, schema, table);
            List<ForeignKeyInfo> fks = listForeignKeys(md, catalog, schema, table);
            Long rowCount = countRows(conn, schema, table);
            String catLabel = pseudoCatalog(connectionId);
            return Optional.of(EntityDescription.builder()
                    .connectionId(null)
                    .dbType(null)
                    .entityKind("table")
                    .catalog(catLabel)
                    .namespace(schema)
                    .entity(table)
                    .columns(withPk)
                    .primaryKey(pk)
                    .foreignKeys(fks)
                    .indexes(indexes)
                    .approximateRowCount(rowCount)
                    .inferredFromSample(false)
                    .notes(List.of())
                    .build());
        } catch (SQLException e) {
            log.warn("describeTable failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private static Long countRows(Connection conn, String schema, String table) {
        try {
            String q = "SELECT COUNT(*) FROM " + quoteId(conn, schema) + "." + quoteId(conn, table);
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(q)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException ignored) {
        }
        return null;
    }

    private static String quoteId(Connection conn, String id) throws SQLException {
        String q = conn.getMetaData().getIdentifierQuoteString();
        if (q == null || q.isBlank() || " ".equals(q)) {
            q = "\"";
        }
        String esc = id.replace(q, q + q);
        return q + esc + q;
    }

    private String pseudoCatalog(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .map(c -> c.getDbName() != null && !c.getDbName().isBlank() ? c.getDbName() : "default")
                .orElse("default");
    }

    private static List<ForeignKeyInfo> listForeignKeys(DatabaseMetaData md, String catalog, String schema, String table)
            throws SQLException {
        List<ForeignKeyInfo> out = new ArrayList<>();
        try (ResultSet rs = md.getImportedKeys(catalog, schema, table)) {
            Map<String, List<String>> byFk = new LinkedHashMap<>();
            Map<String, Map<String, Object>> refs = new LinkedHashMap<>();
            while (rs.next()) {
                String fk = rs.getString("FK_NAME");
                if (fk == null) {
                    fk = "fk";
                }
                byFk.computeIfAbsent(fk, k -> new ArrayList<>()).add(rs.getString("FKCOLUMN_NAME"));
                refs.putIfAbsent(fk, Map.of(
                        "pkTable", rs.getString("PKTABLE_NAME"),
                        "pkSchema", rs.getString("PKTABLE_SCHEM"),
                        "pkColumn", rs.getString("PKCOLUMN_NAME")));
            }
            for (Map.Entry<String, List<String>> e : byFk.entrySet()) {
                out.add(ForeignKeyInfo.builder()
                        .columns(e.getValue())
                        .references(refs.get(e.getKey()))
                        .build());
            }
        }
        return out;
    }

    private static List<IndexInfo> listIndexes(DatabaseMetaData md, String catalog, String schema, String table)
            throws SQLException {
        List<IndexInfo> out = new ArrayList<>();
        try (ResultSet rs = md.getIndexInfo(catalog, schema, table, false, true)) {
            Map<String, List<String>> cols = new LinkedHashMap<>();
            Map<String, Boolean> unique = new LinkedHashMap<>();
            while (rs.next()) {
                String name = rs.getString("INDEX_NAME");
                if (name == null || name.isBlank()) {
                    continue;
                }
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                unique.putIfAbsent(name, !nonUnique);
                String col = rs.getString("COLUMN_NAME");
                if (col != null) {
                    cols.computeIfAbsent(name, k -> new ArrayList<>()).add(col);
                }
            }
            for (Map.Entry<String, List<String>> e : cols.entrySet()) {
                out.add(IndexInfo.builder()
                        .name(e.getKey())
                        .unique(unique.getOrDefault(e.getKey(), false))
                        .columns(e.getValue())
                        .build());
            }
        }
        return out;
    }

    private static QueryResultData readResultSet(ResultSet rs) throws SQLException {
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
        return new QueryResultData(columns, columnTypes, rows);
    }

    private static QueryResultData readResultSetLimited(ResultSet rs, int limit) throws SQLException {
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
        int n = 0;
        while (n < limit && rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= colCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
            n++;
        }
        return new QueryResultData(columns, columnTypes, rows);
    }

    private static boolean isSystemSchema(String dbType, String schema) {
        String s = schema.toUpperCase(Locale.ROOT);
        if ("INFORMATION_SCHEMA".equals(s) || "PG_CATALOG".equals(s)) {
            return true;
        }
        if ("hsqldb".equalsIgnoreCase(dbType)) {
            return "SYSTEM_SCHEMA".equals(s) || "SYS".equals(s);
        }
        if ("derby".equalsIgnoreCase(dbType)) {
            return s.startsWith("SYS") || "SYSCAT".equals(s) || "NULLID".equals(s) || "SQLJ".equals(s);
        }
        if ("h2".equalsIgnoreCase(dbType)) {
            return "INFORMATION_SCHEMA".equals(s);
        }
        return false;
    }

    private static String nullToEmpty(String u) {
        return u != null ? u : "";
    }

    public Map<String, String> getColumnTypes(Long connectionId, String schema, String table) {
        if (schema == null || table == null) {
            return Map.of();
        }
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId))) {
            DatabaseMetaData md = conn.getMetaData();
            Map<String, String> types = new LinkedHashMap<>();
            try (ResultSet rs = md.getColumns(conn.getCatalog(), schema, table, "%")) {
                while (rs.next()) {
                    types.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
                }
            }
            return types;
        } catch (SQLException e) {
            return Map.of();
        }
    }

    public List<String> getPrimaryKeyColumns(Long connectionId, String schema, String table) {
        try (Connection conn = ConnectionSupport.require(getConnection(connectionId))) {
            List<String> pk = new ArrayList<>();
            try (ResultSet rs = conn.getMetaData().getPrimaryKeys(conn.getCatalog(), schema, table)) {
                while (rs.next()) {
                    pk.add(rs.getString("COLUMN_NAME"));
                }
            }
            return pk;
        } catch (SQLException e) {
            return List.of();
        }
    }
}
