package com.panopticum.mssql.service;

import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.util.StringUtils;
import com.panopticum.mssql.repository.MssqlMetadataRepository;
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
public class MssqlMetadataService {

    private static final String MSSQL_PREFIX = "jdbc:sqlserver://";

    private static final Pattern FROM_TABLE = Pattern.compile("(?i)FROM\\s+([^\\s,;()]+)(?:\\s+[Aa][Ss]\\s+[^\\s,;()]+)?(?=[\\s,;(]|$)");

    private final MssqlMetadataRepository mssqlMetadataRepository;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    public Optional<Connection> getConnection(Long connectionId) {
        return mssqlMetadataRepository.getConnection(connectionId);
    }

    public Optional<Connection> getConnection(Long connectionId, String dbName) {
        return mssqlMetadataRepository.getConnection(connectionId, dbName);
    }

    public Optional<String> testConnection(String host, int port, String database, String username, String password) {
        if (host == null || host.isBlank() || database == null || database.isBlank() || username == null || username.isBlank()) {
            return Optional.of("error.specifyHostDbUser");
        }
        String url = MSSQL_PREFIX + host.trim() + ":" + port + ";databaseName=" + database.trim() + ";encrypt=false;trustServerCertificate=true";
        try (Connection c = DriverManager.getConnection(url, username.trim(), password != null ? password : "")) {
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public List<DatabaseInfo> listDatabaseInfos(Long connectionId) {
        return mssqlMetadataRepository.listDatabaseInfos(connectionId);
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

    public List<SchemaInfo> listSchemaInfos(Long connectionId, String dbName) {
        return mssqlMetadataRepository.listSchemaInfos(connectionId, dbName);
    }

    public Page<SchemaInfo> listSchemasPaged(Long connectionId, String dbName, int page, int size, String sort, String order) {
        List<SchemaInfo> all = new ArrayList<>(listSchemaInfos(connectionId, dbName));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<SchemaInfo> comparator;
        if ("tables".equals(sortBy)) {
            comparator = desc ? (a, b) -> Integer.compare(b.getTableCount(), a.getTableCount()) : (a, b) -> Integer.compare(a.getTableCount(), b.getTableCount());
        } else if ("owner".equals(sortBy)) {
            comparator = desc ? (a, b) -> (b.getOwner() != null ? b.getOwner() : "").compareToIgnoreCase(a.getOwner() != null ? a.getOwner() : "")
                    : (a, b) -> (a.getOwner() != null ? a.getOwner() : "").compareToIgnoreCase(b.getOwner() != null ? b.getOwner() : "");
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        List<SchemaInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<TableInfo> listTableInfos(Long connectionId, String dbName, String schema) {
        return mssqlMetadataRepository.listTableInfos(connectionId, dbName, schema);
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
                                              String sortBy, String sortOrder, String search) {
        if (search != null && !search.isBlank()) {
            return executeQueryWithSearch(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, search.trim());
        }
        return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String dbName, String sql, int offset, int limit,
                                              String sortBy, String sortOrder, boolean truncateCells) {
        if (mssqlMetadataRepository.getConnection(connectionId, dbName).isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        String pagedSql = wrapWithLimitOffset(sql.trim(), limit, offset, sortBy, sortOrder);
        Optional<QueryResultData> dataOpt = mssqlMetadataRepository.executeQuery(connectionId, dbName, pagedSql);
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

    private Optional<QueryResult> executeQueryWithSearch(Long connectionId, String dbName, String sql, int offset, int limit,
                                                         String sortBy, String sortOrder, String searchTerm) {
        if (mssqlMetadataRepository.getConnection(connectionId, dbName).isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        String upper = trimmed.toUpperCase().stripLeading();
        if (!upper.startsWith("SELECT") || upper.startsWith("SELECT INTO")) {
            return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
        }
        String innerWithOrder = buildWrappedQueryWithOrder(trimmed, sortBy, sortOrder);
        Optional<QueryResultData> metaOpt = mssqlMetadataRepository.executeQuery(connectionId, dbName,
                innerWithOrder + " OFFSET 0 ROWS FETCH NEXT 0 ROWS ONLY");
        if (metaOpt.isEmpty() || metaOpt.get().getColumns() == null || metaOpt.get().getColumns().isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        List<String> columns = metaOpt.get().getColumns();
        String concatExpr = columns.stream()
                .map(c -> "ISNULL(CAST([_sub].[" + c.replace("]", "]]") + "] AS NVARCHAR(MAX)), N'')")
                .reduce((a, b) -> "CONCAT(" + a + ", N':', " + b + ")")
                .orElse("N''");
        String orderByClause = buildOrderBy(sortBy, sortOrder);
        String searchSql = "SELECT * FROM (" + innerWithOrder + ") AS _sub WHERE " + concatExpr + " LIKE ? " + orderByClause + " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
        String likePattern = "%" + escapeForLike(searchTerm) + "%";
        int maxLimit = Math.min(limit, queryRowsLimit);
        List<Object> params = List.of(likePattern, Math.max(0, offset), maxLimit);
        Optional<QueryResultData> dataOpt = mssqlMetadataRepository.executeQuery(connectionId, dbName, searchSql, params);
        if (dataOpt.isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        QueryResultData data = dataOpt.get();
        boolean hasMore = data.getRows().size() == limit;
        List<List<Object>> rows = data.getRows().size() > limit ? data.getRows().subList(0, limit) : data.getRows();
        List<List<Object>> truncated = new ArrayList<>();
        for (List<Object> row : rows) {
            List<Object> t = new ArrayList<>();
            for (Object cell : row) {
                t.add(StringUtils.truncateCell(cell));
            }
            truncated.add(t);
        }
        return Optional.of(new QueryResult(data.getColumns(), data.getColumnTypes(), truncated, null, null, offset, limit, hasMore));
    }

    private static String escapeForLike(String term) {
        if (term == null) {
            return "";
        }
        return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    private String buildWrappedQueryWithOrder(String trimmed, String sortBy, String sortOrder) {
        return "SELECT * FROM (" + trimmed + ") AS _paged" + buildOrderBy(sortBy, sortOrder);
    }

    private static String buildOrderBy(String sortBy, String sortOrder) {
        if (sortBy != null && !sortBy.isBlank() && sortOrder != null && !sortOrder.isBlank()
                && ("asc".equalsIgnoreCase(sortOrder) || "desc".equalsIgnoreCase(sortOrder))) {
            String quotedCol = "[" + sortBy.replace("]", "]]") + "]";
            return " ORDER BY " + quotedCol + " " + sortOrder.toUpperCase();
        }
        return " ORDER BY 1 ASC";
    }

    private String wrapWithLimitOffset(String sql, int limit, int offset, String sortBy, String sortOrder) {
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        String upper = trimmed.toUpperCase().stripLeading();
        if (!upper.startsWith("SELECT") || upper.startsWith("SELECT INTO")) {
            return sql;
        }
        int maxLimit = Math.min(limit, queryRowsLimit);
        return buildWrappedQueryWithOrder(trimmed, sortBy, sortOrder) + " OFFSET " + Math.max(0, offset) + " ROWS FETCH NEXT " + maxLimit + " ROWS ONLY";
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

    public Map<String, Object> getDetailRow(Long connectionId, String dbName, String schema, String sql, int rowNum,
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
        String schemaName = parseSchemaName(qualified, schema);
        String tableName = parseTableName(qualified, schema);
        if (tableName == null) {
            tableName = qualified.replace("[", "").replace("]", "").trim();
        }
        if (schemaName == null || schemaName.isBlank()) {
            schemaName = "dbo";
        }

        List<String> uniqueKeyColumns = mssqlMetadataRepository.getUniqueKeyColumns(connectionId, dbName, schemaName, tableName);
        boolean editable = !uniqueKeyColumns.isEmpty();

        Map<String, String> columnTypes = mssqlMetadataRepository.getColumnTypes(connectionId, dbName, schemaName, tableName);

        String pagedSql = wrapWithLimitOffset(sql.strip().replaceFirst(";+\\s*$", ""), 1, rowNum, sortBy, sortOrder);
        Optional<Map<String, Object>> rowOpt = mssqlMetadataRepository.executeQuerySingleRow(connectionId, dbName, pagedSql);

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

    private String parseSchemaName(String qualified, String defaultSchema) {
        String normalized = qualified.replace("].[", "\u0000");
        String[] parts = normalized.split("\u0000");
        if (parts.length == 2) {
            return parts[0].replace("[", "").trim();
        }
        if (qualified.contains(".")) {
            String[] dotParts = qualified.split("\\.", 2);
            return dotParts[0].replace("[", "").replace("]", "").trim();
        }
        return defaultSchema;
    }

    private String parseTableName(String qualified, String defaultSchema) {
        String normalized = qualified.replace("].[", "\u0000");
        String[] parts = normalized.split("\u0000");
        if (parts.length == 2) {
            return parts[1].replace("]", "").trim();
        }
        if (qualified.contains(".")) {
            String[] dotParts = qualified.split("\\.", 2);
            return dotParts[1].replace("[", "").replace("]", "").trim();
        }
        return qualified.replace("[", "").replace("]", "").trim();
    }

    public Optional<String> executeUpdateByKey(Long connectionId, String dbName, String schema, String qualifiedTable,
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

        String schemaName = parseSchemaName(qualifiedTable, schema);
        String tableName = parseTableName(qualifiedTable, schema);
        if (schemaName == null || schemaName.isBlank()) {
            schemaName = "dbo";
        }

        String fullTable = "[" + schemaName.replace("]", "]]") + "].[" + tableName.replace("]", "]]") + "]";

        Map<String, String> columnTypes = mssqlMetadataRepository.getColumnTypes(connectionId, dbName, schemaName, tableName);
        Set<String> keySet = Set.copyOf(uniqueKeyColumns);

        List<String> setParts = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        for (Map.Entry<String, String> e : columnValues.entrySet()) {
            if (keySet.contains(e.getKey())) {
                continue;
            }
            String quoted = "[" + e.getKey().replace("]", "]]") + "]";
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
            String quoted = "[" + col.replace("]", "]]") + "]";
            String dataType = columnTypes.get(col);
            String cast = sqlCastForDataType(dataType);
            whereParts.add(quoted + " = " + cast);
            params.add(val);
        }

        String updateSql = "UPDATE " + fullTable + " SET " + String.join(", ", setParts) + " WHERE " + String.join(" AND ", whereParts);
        return mssqlMetadataRepository.executeUpdate(connectionId, dbName, updateSql, params);
    }

    private static String sqlCastForDataType(String dataType) {
        if (dataType == null) {
            return "?";
        }
        return switch (dataType.toLowerCase()) {
            case "bigint", "int", "integer", "smallint", "tinyint" -> "CAST(? AS BIGINT)";
            case "decimal", "numeric", "float", "real" -> "CAST(? AS DECIMAL(38,10))";
            case "date" -> "CAST(? AS DATE)";
            case "datetime", "datetime2", "smalldatetime" -> "CAST(? AS DATETIME2)";
            case "time" -> "CAST(? AS TIME)";
            case "bit" -> "CAST(? AS BIT)";
            default -> "?";
        };
    }

    private static boolean isScalarDataType(String dataType) {
        if (dataType == null) {
            return false;
        }
        String lower = dataType.toLowerCase();
        return lower.matches("(bigint|int|integer|smallint|tinyint|decimal|numeric|float|real|date|datetime|datetime2|smalldatetime|time|bit)");
    }
}
