package com.panopticum.postgres.service;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.postgres.model.TableInfo;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
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

    public List<String> listSchemas(Long connectionId, String dbName, int offset, int limit) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            DatabaseMetaData meta = conn.getMetaData();
            List<String> schemas = new ArrayList<>();
            int skip = 0;
            int maxItems = Math.min(limit, schemasLimit - offset);
            try (ResultSet rs = meta.getSchemas(dbName, null)) {
                while (rs.next() && schemas.size() < maxItems) {
                    String schema = rs.getString("TABLE_SCHEM");
                    if (schema != null && !"information_schema".equals(schema)) {
                        if (skip < offset) {
                            skip++;
                            continue;
                        }
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

    public List<TableInfo> listTables(Long connectionId, String dbName, String schema, int offset, int limit) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return List.of();
            }
            DatabaseMetaData meta = conn.getMetaData();
            List<TableInfo> tables = new ArrayList<>();
            int skip = 0;
            int maxItems = Math.min(limit, tablesLimit - offset);
            try (ResultSet rs = meta.getTables(dbName, schema, "%", new String[]{"TABLE", "VIEW"})) {
                while (rs.next() && tables.size() < maxItems) {
                    String name = rs.getString("TABLE_NAME");
                    String type = rs.getString("TABLE_TYPE");
                    if (name != null) {
                        if (skip < offset) {
                            skip++;
                            continue;
                        }
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

    public Optional<QueryResult> executeQuery(Long connectionId, String sql, int offset, int limit,
                                              String sortBy, String sortOrder) {
        try (Connection conn = getConnection(connectionId).orElse(null)) {
            if (conn == null) {
                return Optional.of(QueryResult.error("Connection not available"));
            }
            String pagedSql = wrapWithLimitOffset(sql.trim(), limit, offset, sortBy, sortOrder);
            try (var stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(pagedSql)) {
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

                boolean hasMore = rows.size() == limit;
                return Optional.of(new QueryResult(columns, rows, null, offset, limit, hasMore));
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
