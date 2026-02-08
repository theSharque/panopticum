package com.panopticum.postgres.service;

import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.util.StringUtils;
import com.panopticum.postgres.model.PgDatabaseInfo;
import com.panopticum.postgres.model.PgQueryResultData;
import com.panopticum.postgres.model.PgSchemaInfo;
import com.panopticum.postgres.model.TableInfo;
import com.panopticum.postgres.repository.PgMetadataRepository;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
@Slf4j
public class PgMetadataService {

    private static final String POSTGRESQL_PREFIX = "jdbc:postgresql://";

    private final PgMetadataRepository pgMetadataRepository;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    @Value("${panopticum.limits.tables:1000}")
    private int tablesLimit;

    public PgMetadataService(PgMetadataRepository pgMetadataRepository) {
        this.pgMetadataRepository = pgMetadataRepository;
    }

    public Optional<Connection> getConnection(Long connectionId) {
        return pgMetadataRepository.getConnection(connectionId);
    }

    public Optional<Connection> getConnection(Long connectionId, String dbName) {
        return pgMetadataRepository.getConnection(connectionId, dbName);
    }

    public Optional<String> testConnection(String host, int port, String dbName, String username, String password) {
        if (host == null || host.isBlank() || dbName == null || dbName.isBlank() || username == null || username.isBlank()) {
            return Optional.of("error.specifyHostDbUser");
        }
        String url = POSTGRESQL_PREFIX + host.trim() + ":" + port + "/" + dbName.trim();
        try (Connection c = DriverManager.getConnection(url, username.trim(), password != null ? password : "")) {
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public List<String> listDatabases(Long connectionId) {
        return pgMetadataRepository.listDatabaseInfos(connectionId).stream().map(PgDatabaseInfo::getName).toList();
    }

    public List<PgDatabaseInfo> listDatabaseInfos(Long connectionId) {
        return pgMetadataRepository.listDatabaseInfos(connectionId);
    }

    public Page<PgDatabaseInfo> listDatabasesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<PgDatabaseInfo> all = new ArrayList<>(listDatabaseInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<PgDatabaseInfo> comparator = "size".equals(sortBy)
                ? (desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk()))
                : (desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        List<PgDatabaseInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<String> listSchemas(Long connectionId, String dbName, int offset, int limit) {
        return pgMetadataRepository.listSchemaInfos(connectionId, dbName).stream().map(PgSchemaInfo::getName).toList();
    }

    public List<PgSchemaInfo> listSchemaInfos(Long connectionId, String dbName) {
        return pgMetadataRepository.listSchemaInfos(connectionId, dbName);
    }

    public Page<PgSchemaInfo> listSchemasPaged(Long connectionId, String dbName, int page, int size, String sort, String order) {
        List<PgSchemaInfo> all = new ArrayList<>(listSchemaInfos(connectionId, dbName));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<PgSchemaInfo> comparator;
        if ("tables".equals(sortBy)) {
            comparator = desc ? (a, b) -> Integer.compare(b.getTableCount(), a.getTableCount()) : (a, b) -> Integer.compare(a.getTableCount(), b.getTableCount());
        } else if ("owner".equals(sortBy)) {
            comparator = desc ? (a, b) -> (b.getOwner() != null ? b.getOwner() : "").compareToIgnoreCase(a.getOwner() != null ? a.getOwner() : "")
                    : (a, b) -> (a.getOwner() != null ? a.getOwner() : "").compareToIgnoreCase(b.getOwner() != null ? b.getOwner() : "");
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        List<PgSchemaInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<TableInfo> listTables(Long connectionId, String dbName, String schema, int offset, int limit) {
        List<TableInfo> all = pgMetadataRepository.listTableInfos(connectionId, dbName, schema);
        int end = Math.min(offset + Math.min(limit, tablesLimit - offset), all.size());
        return offset < all.size() ? all.subList(offset, end) : List.of();
    }

    public List<TableInfo> listTableInfos(Long connectionId, String dbName, String schema) {
        return pgMetadataRepository.listTableInfos(connectionId, dbName, schema);
    }

    public Page<TableInfo> listTablesPaged(Long connectionId, String dbName, String schema, int page, int size, String sort, String order) {
        List<TableInfo> all = new ArrayList<>(listTableInfos(connectionId, dbName, schema));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<TableInfo> comparator;
        if ("type".equalsIgnoreCase(sortBy)) {
            comparator = desc ? (a, b) -> (b.getType() != null ? b.getType() : "").compareToIgnoreCase(a.getType() != null ? a.getType() : "")
                    : (a, b) -> (a.getType() != null ? a.getType() : "").compareToIgnoreCase(b.getType() != null ? b.getType() : "");
        } else if ("size".equals(sortBy)) {
            comparator = desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk());
        } else if ("rows".equals(sortBy)) {
            comparator = desc ? (a, b) -> Long.compare(b.getApproximateRowCount(), a.getApproximateRowCount()) : (a, b) -> Long.compare(a.getApproximateRowCount(), b.getApproximateRowCount());
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        List<TableInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                              String sortBy, String sortOrder) {
        return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                              String sortBy, String sortOrder, boolean truncateCells) {
        if (pgMetadataRepository.getConnection(connectionId, dbName).isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        String pagedSql = wrapWithLimitOffset(sql.trim(), limit, offset, sortBy, sortOrder);
        Optional<PgQueryResultData> dataOpt = pgMetadataRepository.executeQuery(connectionId, dbName, pagedSql);
        if (dataOpt.isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        PgQueryResultData data = dataOpt.get();
        boolean hasMore = data.getRows().size() == limit;
        List<List<Object>> rows = data.getRows().size() > limit ? data.getRows().subList(0, limit) : data.getRows();
        if (truncateCells) {
            List<List<Object>> truncated = new ArrayList<>();
            for (List<Object> row : rows) {
                List<Object> t = new ArrayList<>();
                for (Object cell : row) {
                    t.add(StringUtils.truncateCell(cell));
                }
                truncated.add(t);
            }
            rows = truncated;
        }
        return Optional.of(new QueryResult(data.getColumns(), data.getColumnTypes(), rows, null, null, offset, limit, hasMore));
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

    private static final Pattern FROM_TABLE = Pattern.compile("(?i)FROM\\s+([^\\s,;()]+)(?:\\s+[Aa][Ss]\\s+[^\\s,;()]+)?(?=[\\s,;(]|$)");

    public Optional<String> parseTableFromSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return Optional.empty();
        }
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        Matcher m = FROM_TABLE.matcher(trimmed);
        if (!m.find()) {
            return Optional.empty();
        }
        String ref = m.group(1).trim();
        int asIdx = ref.toUpperCase().indexOf(" AS ");
        String tableRef = asIdx >= 0 ? ref.substring(0, asIdx).trim() : ref;
        return tableRef.isBlank() ? Optional.empty() : Optional.of(tableRef);
    }

    public Map<String, Object> getDetailRowWithCtid(Long connectionId, String dbName, String schema, String sql,
                                                   int rowNum, String sortBy, String sortOrder) {
        Map<String, Object> out = new LinkedHashMap<>();
        Optional<String> tableRef = parseTableFromSql(sql);

        if (tableRef.isEmpty()) {
            out.put("error", "Could not determine table from SQL.");
            return out;
        }

        String qualified = tableRef.get();
        String schemaName = schema;
        String tableName = null;
        String normalized = qualified.replace("\".\"", "\u0000");
        String[] parts = normalized.split("\u0000");

        if (parts.length == 2) {
            schemaName = parts[0].replace("\"", "").trim();
            tableName = parts[1].replace("\"", "").trim();
        } else if (qualified.contains(".")) {
            String[] dotParts = qualified.split("\\.", 2);
            schemaName = dotParts[0].replace("\"", "").trim();
            tableName = dotParts[1].replace("\"", "").trim();
        } else {
            tableName = qualified.replace("\"", "").trim();
        }

        Map<String, String> columnTypes = (schemaName != null && tableName != null)
                ? pgMetadataRepository.getColumnTypes(connectionId, dbName, schemaName, tableName)
                : Map.of();

        String modifiedSql = sql.strip().replaceFirst(";+\\s*$", "")
                .replaceFirst("(?i)SELECT\\s+\\*", "SELECT *, " + qualified + ".ctid");
        String pagedSql = wrapWithLimitOffset(modifiedSql, 1, rowNum, sortBy, sortOrder);
        Optional<Map<String, Object>> rowOpt = pgMetadataRepository.executeQuerySingleRow(connectionId, dbName, pagedSql);

        if (rowOpt.isEmpty()) {
            out.put("error", "error.connectionNotAvailable");
            return out;
        }

        Map<String, Object> row = rowOpt.get();
        if (row.isEmpty()) {
            out.put("error", "No row at this position.");
            return out;
        }

        List<Map<String, String>> detailRows = new ArrayList<>();
        String rowCtid = null;
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if ("ctid".equalsIgnoreCase(e.getKey())) {
                rowCtid = e.getValue() != null ? e.getValue().toString() : null;
                continue;
            }
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("name", e.getKey());
            entry.put("type", columnTypes.getOrDefault(e.getKey(), "unknown"));
            entry.put("value", e.getValue() != null ? e.getValue().toString() : "");
            detailRows.add(entry);
        }

        out.put("detailRows", detailRows);
        out.put("rowCtid", rowCtid != null ? rowCtid : "");

        return out;
    }

    public Optional<String> executeUpdateByCtid(Long connectionId, String dbName, String qualifiedTable,
                                                String ctid, Map<String, String> columnValues) {
        if (ctid == null || ctid.isBlank() || qualifiedTable == null || qualifiedTable.isBlank()) {
            return Optional.of("Missing ctid or table.");
        }

        if (columnValues == null || columnValues.isEmpty()) {
            return Optional.empty();
        }

        String schemaName = null;
        String tableName = qualifiedTable.replace("\"", "").trim();
        String normalized = qualifiedTable.replace("\".\"", "\u0000");
        String[] parts = normalized.split("\u0000");

        if (parts.length == 2) {
            schemaName = parts[0].replace("\"", "").trim();
            tableName = parts[1].replace("\"", "").trim();
        } else if (qualifiedTable.contains(".")) {
            String[] dotParts = qualifiedTable.split("\\.", 2);
            schemaName = dotParts[0].replace("\"", "").trim();
            tableName = dotParts[1].replace("\"", "").trim();
        }

        Map<String, String> columnTypes = (schemaName != null && tableName != null)
                ? pgMetadataRepository.getColumnTypes(connectionId, dbName, schemaName, tableName)
                : Map.of();
        List<String> setParts = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (Map.Entry<String, String> e : columnValues.entrySet()) {
            if ("ctid".equalsIgnoreCase(e.getKey())) {
                continue;
            }
            String quoted = "\"" + e.getKey().replace("\"", "\"\"") + "\"";
            String dataType = columnTypes.get(e.getKey());
            String cast = sqlCastForDataType(dataType);
            setParts.add(quoted + " = " + cast);
            String raw = e.getValue();
            values.add((raw == null || raw.isBlank()) && isScalarDataType(dataType) ? null : (raw != null ? raw : ""));
        }

        if (setParts.isEmpty()) {
            return Optional.empty();
        }

        if (tableName == null || tableName.isBlank()) {
            return Optional.of("Invalid table.");
        }

        String trimmedCtid = ctid.trim();
        if (!trimmedCtid.matches("\\(\\d+\\s*,\\s*\\d+\\)")) {
            return Optional.of("Invalid ctid format.");
        }

        String qualified = (schemaName != null && !schemaName.isBlank())
                ? "\"" + schemaName.replace("\"", "\"\"") + "\".\"" + tableName.replace("\"", "\"\"") + "\""
                : "\"" + tableName.replace("\"", "\"\"") + "\"";
        String updateSql = "UPDATE " + qualified + " SET " + String.join(", ", setParts) + " WHERE ctid = '" + trimmedCtid + "'::tid";

        return pgMetadataRepository.executeUpdate(connectionId, dbName, updateSql, values);
    }

    private static String sqlCastForDataType(String dataType) {
        if (dataType == null) {
            return "?";
        }
        return switch (dataType) {
            case "bigint", "integer", "smallint", "numeric" -> "?::" + dataType;
            case "double precision" -> "?::double precision";
            case "real" -> "?::real";
            case "timestamp without time zone" -> "?::timestamp";
            case "timestamp with time zone" -> "?::timestamptz";
            case "date" -> "?::date";
            case "time without time zone" -> "?::time";
            case "time with time zone" -> "?::timetz";
            case "boolean" -> "?::boolean";
            default -> "?";
        };
    }

    private static boolean isScalarDataType(String dataType) {
        if (dataType == null) {
            return false;
        }
        return switch (dataType) {
            case "bigint", "integer", "smallint", "numeric", "double precision", "real",
                    "timestamp without time zone", "timestamp with time zone", "date",
                    "time without time zone", "time with time zone", "boolean" -> true;
            default -> false;
        };
    }
}
