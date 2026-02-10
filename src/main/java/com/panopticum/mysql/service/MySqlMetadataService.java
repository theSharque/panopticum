package com.panopticum.mysql.service;

import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.util.StringUtils;
import com.panopticum.mysql.repository.MySqlMetadataRepository;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class MySqlMetadataService {

    private static final String MYSQL_PREFIX = "jdbc:mysql://";

    private static final Pattern FROM_TABLE = Pattern.compile("(?i)FROM\\s+([^\\s,;()]+)(?:\\s+[Aa][Ss]\\s+[^\\s,;()]+)?(?=[\\s,;(]|$)");

    private final MySqlMetadataRepository mySqlMetadataRepository;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    public Optional<Connection> getConnection(Long connectionId) {
        return mySqlMetadataRepository.getConnection(connectionId);
    }

    public Optional<Connection> getConnection(Long connectionId, String dbName) {
        return mySqlMetadataRepository.getConnection(connectionId, dbName);
    }

    public Optional<String> testConnection(String host, int port, String database, String username, String password) {
        if (host == null || host.isBlank() || database == null || database.isBlank() || username == null || username.isBlank()) {
            return Optional.of("error.specifyHostDbUser");
        }
        String url = MYSQL_PREFIX + host.trim() + ":" + port + "/" + database.trim();
        try (Connection c = DriverManager.getConnection(url, username.trim(), password != null ? password : "")) {
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public List<DatabaseInfo> listDatabaseInfos(Long connectionId) {
        return mySqlMetadataRepository.listDatabaseInfos(connectionId);
    }

    public Page<DatabaseInfo> listDatabasesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<DatabaseInfo> all = new ArrayList<>(listDatabaseInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<DatabaseInfo> comparator = "size".equals(sortBy)
                ? (desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk()))
                : (desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        List<DatabaseInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<TableInfo> listTableInfos(Long connectionId, String dbName) {
        return mySqlMetadataRepository.listTableInfos(connectionId, dbName);
    }

    public Page<TableInfo> listTablesPaged(Long connectionId, String dbName, int page, int size, String sort, String order) {
        List<TableInfo> all = new ArrayList<>(listTableInfos(connectionId, dbName));
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

    public Optional<@NonNull QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                                       String sortBy, String sortOrder) {
        return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<@NonNull QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                                       String sortBy, String sortOrder, boolean truncateCells) {
        if (mySqlMetadataRepository.getConnection(connectionId, dbName).isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        String pagedSql = wrapWithLimitOffset(sql.trim(), limit, offset, sortBy, sortOrder);
        Optional<QueryResultData> dataOpt = mySqlMetadataRepository.executeQuery(connectionId, dbName, pagedSql);
        if (dataOpt.isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        QueryResultData data = dataOpt.get();
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
            String quotedCol = "`" + sortBy.replace("`", "``") + "`";
            orderBy = " ORDER BY " + quotedCol + " " + sortOrder.toUpperCase();
        } else {
            orderBy = " ORDER BY 1 ASC";
        }
        return "SELECT * FROM (" + trimmed + ") AS _paged" + orderBy + " LIMIT " + maxLimit + " OFFSET " + Math.max(0, offset);
    }

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

    public Map<String, Object> getDetailRow(Long connectionId, String dbName, String sql, int rowNum,
                                            String sortBy, String sortOrder) {
        Map<String, Object> out = new LinkedHashMap<>();
        Optional<String> tableRef = parseTableFromSql(sql);

        if (tableRef.isEmpty()) {
            out.put("error", "Could not determine table from SQL.");
            out.put("editable", false);
            out.put("detailRows", List.<Map<String, String>>of());
            return out;
        }

        String qualified = tableRef.get();
        String tableName = parseTableName(qualified, dbName);
        if (tableName == null) {
            tableName = qualified.replace("`", "").trim();
        }

        List<String> uniqueKeyColumns = mySqlMetadataRepository.getUniqueKeyColumns(connectionId, dbName, tableName);
        boolean editable = !uniqueKeyColumns.isEmpty();

        Map<String, String> columnTypes = mySqlMetadataRepository.getColumnTypes(connectionId, dbName, tableName);

        String pagedSql = wrapWithLimitOffset(sql.strip().replaceFirst(";+\\s*$", ""), 1, rowNum, sortBy, sortOrder);
        Optional<Map<String, Object>> rowOpt = mySqlMetadataRepository.executeQuerySingleRow(connectionId, dbName, pagedSql);

        if (rowOpt.isEmpty()) {
            out.put("error", "error.connectionNotAvailable");
            out.put("editable", false);
            out.put("detailRows", List.<Map<String, String>>of());
            return out;
        }

        Map<String, Object> row = rowOpt.get();
        if (row.isEmpty()) {
            out.put("error", "No row at this position.");
            out.put("editable", false);
            out.put("detailRows", List.<Map<String, String>>of());
            return out;
        }

        List<Map<String, String>> detailRows = new ArrayList<>();
        Set<String> keySet = Set.copyOf(uniqueKeyColumns);
        for (Map.Entry<String, Object> e : row.entrySet()) {
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("name", e.getKey());
            entry.put("type", columnTypes.getOrDefault(e.getKey(), "unknown"));
            entry.put("value", e.getValue() != null ? e.getValue().toString() : "");
            entry.put("readOnly", String.valueOf(keySet.contains(e.getKey())));
            detailRows.add(entry);
        }

        out.put("detailRows", detailRows);
        out.put("editable", editable);
        out.put("uniqueKeyColumns", uniqueKeyColumns);
        out.put("uniqueKeyColumnsJoined", String.join(",", uniqueKeyColumns));
        out.put("qualifiedTable", qualified);

        return out;
    }

    private String parseTableName(String qualified, String defaultSchema) {
        String normalized = qualified.replace("`.`", "\u0000");
        String[] parts = normalized.split("\u0000");
        if (parts.length == 2) {
            return parts[1].replace("`", "").trim();
        }
        if (qualified.contains(".")) {
            String[] dotParts = qualified.split("\\.", 2);
            return dotParts[1].replace("`", "").trim();
        }
        return qualified.replace("`", "").trim();
    }

    public Optional<String> executeUpdateByKey(Long connectionId, String dbName, String qualifiedTable,
                                               List<String> uniqueKeyColumns, Map<String, Object> keyValues,
                                               Map<String, String> columnValues) {
        if (uniqueKeyColumns == null || uniqueKeyColumns.isEmpty()) {
            return Optional.of("No primary key or unique index.");
        }
        if (qualifiedTable == null || qualifiedTable.isBlank()) {
            return Optional.of("Invalid table.");
        }
        if (columnValues == null || columnValues.isEmpty()) {
            return Optional.empty();
        }

        String tableName = parseTableName(qualifiedTable, dbName);
        String fullTable = (dbName != null && !dbName.isBlank())
                ? "`" + dbName.replace("`", "``") + "`.`" + tableName.replace("`", "``") + "`"
                : "`" + tableName.replace("`", "``") + "`";

        Map<String, String> columnTypes = mySqlMetadataRepository.getColumnTypes(connectionId, dbName, tableName);
        Set<String> keySet = Set.copyOf(uniqueKeyColumns);

        List<String> setParts = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        for (Map.Entry<String, String> e : columnValues.entrySet()) {
            if (keySet.contains(e.getKey())) {
                continue;
            }
            String quoted = "`" + e.getKey().replace("`", "``") + "`";
            String dataType = columnTypes.get(e.getKey());
            String cast = sqlCastForDataType(dataType);
            setParts.add(quoted + " = " + cast);
            String raw = e.getValue();
            params.add((raw == null || raw.isBlank()) && isScalarDataType(dataType) ? null : (raw != null ? raw : ""));
        }

        if (setParts.isEmpty()) {
            return Optional.empty();
        }

        List<String> whereParts = new ArrayList<>();
        for (String col : uniqueKeyColumns) {
            Object val = keyValues != null ? keyValues.get(col) : null;
            if (val == null) {
                return Optional.of("Missing key value for " + col);
            }
            String quoted = "`" + col.replace("`", "``") + "`";
            String dataType = columnTypes.get(col);
            String cast = sqlCastForDataType(dataType);
            whereParts.add(quoted + " = " + cast);
            params.add(val);
        }

        String updateSql = "UPDATE " + fullTable + " SET " + String.join(", ", setParts) + " WHERE " + String.join(" AND ", whereParts);
        return mySqlMetadataRepository.executeUpdate(connectionId, dbName, updateSql, params);
    }

    private static String sqlCastForDataType(String dataType) {
        if (dataType == null) {
            return "?";
        }
        return switch (dataType.toLowerCase()) {
            case "bigint", "int", "integer", "smallint", "tinyint", "mediumint" -> "CAST(? AS SIGNED)";
            case "decimal", "numeric", "float", "double" -> "CAST(? AS DECIMAL(65,30))";
            case "date" -> "CAST(? AS DATE)";
            case "datetime", "timestamp" -> "CAST(? AS DATETIME)";
            case "time" -> "CAST(? AS TIME)";
            case "year" -> "CAST(? AS SIGNED)";
            case "bit" -> "CAST(? AS SIGNED)";
            default -> "?";
        };
    }

    private static boolean isScalarDataType(String dataType) {
        if (dataType == null) {
            return false;
        }
        String lower = dataType.toLowerCase();
        return lower.matches("(bigint|int|integer|smallint|tinyint|mediumint|decimal|numeric|float|double|date|datetime|timestamp|time|year|bit)");
    }
}
