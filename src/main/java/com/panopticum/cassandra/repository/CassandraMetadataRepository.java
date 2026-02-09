package com.panopticum.cassandra.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.cassandra.model.CassandraKeyspaceInfo;
import com.panopticum.cassandra.model.CassandraQueryResultData;
import com.panopticum.cassandra.model.CassandraTableInfo;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class CassandraMetadataRepository {

    private static final int DEFAULT_PORT = 9042;
    private static final String LOCAL_DC = "datacenter1";

    private static final String LIST_KEYSPACES_CQL =
            "SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces";
    private static final String LIST_TABLES_CQL =
            "SELECT table_name, comment, default_time_to_live, gc_grace_seconds FROM system_schema.tables WHERE keyspace_name = ?";
    private static final String LIST_COLUMNS_CQL =
            "SELECT column_name, kind, position, type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?";

    private final DbConnectionService dbConnectionService;

    public List<CassandraKeyspaceInfo> listKeyspaceInfos(Long connectionId) {
        Optional<CqlSession> sessionOpt = createSession(connectionId, null);
        if (sessionOpt.isEmpty()) {
            return List.of();
        }
        try (CqlSession session = sessionOpt.get()) {
            ResultSet rs = session.execute(SimpleStatement.newInstance(LIST_KEYSPACES_CQL));
            List<CassandraKeyspaceInfo> list = new ArrayList<>();
            for (Row row : rs) {
                String name = row.getString("keyspace_name");
                if (name != null) {
                    Boolean durableWrites = row.getBoolean("durable_writes");
                    String replicationFormatted = formatReplication(row.getMap("replication", String.class, String.class));
                    list.add(new CassandraKeyspaceInfo(name, durableWrites, replicationFormatted));
                }
            }
            return list;
        } catch (Exception e) {
            log.warn("listKeyspaceInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    public List<CassandraTableInfo> listTableInfos(Long connectionId, String keyspaceName) {
        if (keyspaceName == null || keyspaceName.isBlank()) {
            return List.of();
        }
        Optional<CqlSession> sessionOpt = createSession(connectionId, keyspaceName);
        if (sessionOpt.isEmpty()) {
            return List.of();
        }
        try (CqlSession session = sessionOpt.get()) {
            ResultSet rs = session.execute(SimpleStatement.newInstance(LIST_TABLES_CQL, keyspaceName));
            List<CassandraTableInfo> list = new ArrayList<>();
            for (Row row : rs) {
                String name = row.getString("table_name");
                if (name != null) {
                    String comment = row.getString("comment");
                    Integer ttl = row.isNull("default_time_to_live") ? null : row.getInt("default_time_to_live");
                    Integer gcGrace = row.isNull("gc_grace_seconds") ? null : row.getInt("gc_grace_seconds");
                    list.add(new CassandraTableInfo(name, "table", comment, ttl, gcGrace));
                }
            }
            return list;
        } catch (Exception e) {
            log.warn("listTableInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    public Optional<CassandraQueryResultData> executeCql(Long connectionId, String keyspaceName, String cql, int limit) {
        Optional<CqlSession> sessionOpt = createSession(connectionId, keyspaceName != null && !keyspaceName.isBlank() ? keyspaceName : null);
        if (sessionOpt.isEmpty()) {
            return Optional.empty();
        }
        try (CqlSession session = sessionOpt.get()) {
            if (cql == null || cql.isBlank()) {
                return Optional.empty();
            }
            String trimmed = cql.trim();
            SimpleStatement stmt = SimpleStatement.newInstance(trimmed).setPageSize(limit);
            ResultSet rs = session.execute(stmt);
            var defs = rs.getColumnDefinitions();
            if (defs == null || defs.size() == 0) {
                return Optional.of(new CassandraQueryResultData(List.of(), List.of(), List.of()));
            }
            int colCount = defs.size();
            List<String> columns = new ArrayList<>();
            List<String> columnTypes = new ArrayList<>();
            for (int i = 0; i < colCount; i++) {
                columns.add(defs.get(i).getName().asInternal());
                columnTypes.add(defs.get(i).getType().toString());
            }
            List<List<Object>> rows = new ArrayList<>();
            for (Row row : rs) {
                List<Object> cellList = new ArrayList<>();
                for (int i = 0; i < colCount; i++) {
                    cellList.add(row.getObject(i));
                }
                rows.add(cellList);
            }
            return Optional.of(new CassandraQueryResultData(columns, columnTypes, rows));
        } catch (Exception e) {
            log.warn("executeCql failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public List<String> getPrimaryKeyColumns(Long connectionId, String keyspaceName, String tableName) {
        if (keyspaceName == null || keyspaceName.isBlank() || tableName == null || tableName.isBlank()) {
            return List.of();
        }
        Optional<CqlSession> sessionOpt = createSession(connectionId, null);
        if (sessionOpt.isEmpty()) {
            return List.of();
        }
        try (CqlSession session = sessionOpt.get()) {
            ResultSet rs = session.execute(SimpleStatement.newInstance(LIST_COLUMNS_CQL, keyspaceName, tableName));
            List<Row> rows = new ArrayList<>();
            rs.forEach(rows::add);
            return rows.stream()
                    .filter(r -> {
                        String kind = r.getString("kind");
                        return "partition_key".equals(kind) || "clustering".equals(kind);
                    })
                    .sorted((a, b) -> Integer.compare(a.getInt("position"), b.getInt("position")))
                    .map(r -> r.getString("column_name"))
                    .filter(n -> n != null)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.warn("getPrimaryKeyColumns failed: {}", e.getMessage());
            return List.of();
        }
    }

    public Map<String, String> getColumnTypes(Long connectionId, String keyspaceName, String tableName) {
        if (keyspaceName == null || keyspaceName.isBlank() || tableName == null || tableName.isBlank()) {
            return Map.of();
        }
        Optional<CqlSession> sessionOpt = createSession(connectionId, null);
        if (sessionOpt.isEmpty()) {
            return Map.of();
        }
        try (CqlSession session = sessionOpt.get()) {
            ResultSet rs = session.execute(SimpleStatement.newInstance(LIST_COLUMNS_CQL, keyspaceName, tableName));
            Map<String, String> out = new LinkedHashMap<>();
            for (Row row : rs) {
                String col = row.getString("column_name");
                String type = row.getString("type");
                if (col != null) {
                    out.put(col, type != null ? type : "text");
                }
            }
            return out;
        } catch (Exception e) {
            log.warn("getColumnTypes failed: {}", e.getMessage());
            return Map.of();
        }
    }

    public Optional<String> executeUpdate(Long connectionId, String keyspaceName, String tableName,
                                         List<String> primaryKeyColumns,
                                         Map<String, Object> keyValues,
                                         Map<String, String> columnValues,
                                         Map<String, String> columnTypes) {
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty() || keyValues == null || columnValues == null
                || columnTypes == null || keyspaceName == null || keyspaceName.isBlank()
                || tableName == null || tableName.isBlank()) {
            return Optional.of("Invalid arguments.");
        }
        Set<String> keySet = Set.copyOf(primaryKeyColumns);
        List<String> setCols = columnValues.keySet().stream().filter(c -> !keySet.contains(c)).collect(Collectors.toList());
        if (setCols.isEmpty()) {
            return Optional.empty();
        }
        Optional<CqlSession> sessionOpt = createSession(connectionId, keyspaceName);
        if (sessionOpt.isEmpty()) {
            return Optional.of("Connection not available.");
        }
        try (CqlSession session = sessionOpt.get()) {
            String ks = quoteId(keyspaceName);
            String tbl = quoteId(tableName);
            List<String> setParts = new ArrayList<>();
            for (String col : setCols) {
                setParts.add(quoteId(col) + " = ?");
            }
            List<String> whereParts = new ArrayList<>();
            for (String col : primaryKeyColumns) {
                whereParts.add(quoteId(col) + " = ?");
            }
            String cql = "UPDATE " + ks + "." + tbl + " SET " + String.join(", ", setParts) + " WHERE " + String.join(" AND ", whereParts);
            PreparedStatement prepared = session.prepare(cql);
            int idx = 0;
            BoundStatementBuilder builder = prepared.boundStatementBuilder();
            for (String col : setCols) {
                String type = columnTypes.getOrDefault(col, "text");
                String val = columnValues.get(col);
                builder = bindValue(builder, idx++, type, val);
            }
            for (String col : primaryKeyColumns) {
                Object val = keyValues.get(col);
                String type = columnTypes.getOrDefault(col, "text");
                String strVal = val != null ? val.toString() : null;
                builder = bindValue(builder, idx++, type, strVal);
            }
            session.execute(builder.build());
            return Optional.empty();
        } catch (Exception e) {
            log.warn("executeUpdate failed: {}", e.getMessage());
            return Optional.of(e.getMessage());
        }
    }

    private static String quoteId(String id) {
        if (id == null) {
            return "\"\"";
        }
        return "\"" + id.replace("\"", "\"\"") + "\"";
    }

    private static BoundStatementBuilder bindValue(BoundStatementBuilder builder, int index, String cqlType, String value) {
        if (cqlType == null) {
            cqlType = "text";
        }
        String type = cqlType.toLowerCase().split("<")[0].trim();
        switch (type) {
            case "ascii":
            case "text":
            case "varchar":
                return builder.setString(index, value);
            case "int":
            case "smallint":
            case "tinyint":
                if (value == null || value.isBlank()) {
                    return builder.setToNull(index);
                }
                try {
                    return builder.setInt(index, Integer.parseInt(value.trim()));
                } catch (NumberFormatException e) {
                    return builder.setToNull(index);
                }
            case "bigint":
            case "counter":
                if (value == null || value.isBlank()) {
                    return builder.setToNull(index);
                }
                try {
                    return builder.setLong(index, Long.parseLong(value.trim()));
                } catch (NumberFormatException e) {
                    return builder.setToNull(index);
                }
            case "boolean":
                if (value == null || value.isBlank()) {
                    return builder.setToNull(index);
                }
                return builder.setBoolean(index, "true".equalsIgnoreCase(value.trim()) || "1".equals(value.trim()));
            case "float":
                if (value == null || value.isBlank()) {
                    return builder.setToNull(index);
                }
                try {
                    return builder.setFloat(index, Float.parseFloat(value.trim()));
                } catch (NumberFormatException e) {
                    return builder.setToNull(index);
                }
            case "double":
                if (value == null || value.isBlank()) {
                    return builder.setToNull(index);
                }
                try {
                    return builder.setDouble(index, Double.parseDouble(value.trim()));
                } catch (NumberFormatException e) {
                    return builder.setToNull(index);
                }
            case "uuid":
            case "timeuuid":
                if (value == null || value.isBlank()) {
                    return builder.setToNull(index);
                }
                try {
                    return builder.setUuid(index, UUID.fromString(value.trim()));
                } catch (IllegalArgumentException e) {
                    return builder.setToNull(index);
                }
            case "timestamp":
            case "date":
                if (value == null || value.isBlank()) {
                    return builder.setToNull(index);
                }
                try {
                    long ms = Long.parseLong(value.trim());
                    return builder.setInstant(index, Instant.ofEpochMilli(ms));
                } catch (NumberFormatException e) {
                    return builder.setInstant(index, Instant.parse(value.trim()));
                } catch (Exception e) {
                    return builder.setToNull(index);
                }
            default:
                return builder.setString(index, value);
        }
    }

    private Optional<CqlSession> createSession(Long connectionId, String keyspace) {
        return dbConnectionService.findById(connectionId)
                .filter(c -> "cassandra".equalsIgnoreCase(c.getType()))
                .flatMap(c -> buildSession(c, keyspace));
    }

    private Optional<CqlSession> buildSession(DbConnection conn, String keyspace) {
        int port = conn.getPort() > 0 ? conn.getPort() : DEFAULT_PORT;
        try {
            var builder = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(conn.getHost().trim(), port))
                    .withLocalDatacenter(LOCAL_DC);
            if (conn.getUsername() != null && !conn.getUsername().isBlank()) {
                builder = builder.withAuthCredentials(conn.getUsername(), conn.getPassword() != null ? conn.getPassword() : "");
            }
            if (keyspace != null && !keyspace.isBlank()) {
                builder = builder.withKeyspace(keyspace);
            }
            return Optional.of(builder.build());
        } catch (Exception e) {
            log.warn("Failed to connect to {}: {}", conn.getName(), e.getMessage());
            return Optional.empty();
        }
    }

    private static String formatReplication(Map<String, String> replication) {
        if (replication == null || replication.isEmpty()) {
            return "";
        }
        String clazz = replication.get("class");
        if (clazz != null) {
            String shortClass = clazz.contains(".") ? clazz.substring(clazz.lastIndexOf('.') + 1) : clazz;
            if ("SimpleStrategy".equals(shortClass)) {
                String rf = replication.get("replication_factor");
                return rf != null ? shortClass + ", RF=" + rf : shortClass;
            }
            String opts = replication.entrySet().stream()
                    .filter(e -> !"class".equals(e.getKey()))
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(", "));
            return opts.isEmpty() ? shortClass : shortClass + " (" + opts + ")";
        }
        return replication.toString();
    }

    public Optional<CqlSession> createSessionForTest(String host, int port, String keyspace, String username, String password) {
        try {
            var builder = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(host.trim(), port > 0 ? port : DEFAULT_PORT))
                    .withLocalDatacenter(LOCAL_DC);
            if (username != null && !username.isBlank()) {
                builder = builder.withAuthCredentials(username, password != null ? password : "");
            }
            if (keyspace != null && !keyspace.isBlank()) {
                builder = builder.withKeyspace(keyspace);
            }
            return Optional.of(builder.build());
        } catch (Exception e) {
            log.debug("Test connection failed: {}", e.getMessage());
            return Optional.empty();
        }
    }
}
