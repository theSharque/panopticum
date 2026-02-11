package com.panopticum.oracle.service;

import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.util.StringUtils;
import com.panopticum.oracle.repository.OracleMetadataRepository;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class OracleMetadataService {

    private static final String ORACLE_PREFIX = "jdbc:oracle:thin:@//";

    private static final Pattern FROM_TABLE = Pattern.compile("(?i)FROM\\s+([^\\s,;()]+)(?:\\s+[Aa][Ss]\\s+[^\\s,;()]+)?(?=[\\s,;(]|$)");

    private final OracleMetadataRepository oracleMetadataRepository;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    public Optional<Connection> getConnection(Long connectionId) {
        return oracleMetadataRepository.getConnection(connectionId);
    }

    public Optional<Connection> getConnection(Long connectionId, String schema) {
        return oracleMetadataRepository.getConnection(connectionId, schema);
    }

    public Optional<String> testConnection(String host, int port, String serviceName, String username, String password) {
        if (host == null || host.isBlank() || serviceName == null || serviceName.isBlank()
                || username == null || username.isBlank()) {
            return Optional.of("error.specifyHostDbUser");
        }
        String url = ORACLE_PREFIX + host.trim() + ":" + port + "/" + serviceName.trim();
        try (Connection c = DriverManager.getConnection(url, username.trim(), password != null ? password : "")) {
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public List<SchemaInfo> listSchemaInfos(Long connectionId) {
        return oracleMetadataRepository.listSchemaInfos(connectionId);
    }

    public Page<SchemaInfo> listSchemasPaged(Long connectionId, int page, int size, String sort, String order) {
        List<SchemaInfo> all = new ArrayList<>(listSchemaInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<SchemaInfo> comparator;
        if ("tables".equals(sortBy)) {
            comparator = desc ? (a, b) -> Integer.compare(b.getTableCount(), a.getTableCount())
                    : (a, b) -> Integer.compare(a.getTableCount(), b.getTableCount());
        } else if ("owner".equals(sortBy)) {
            comparator = desc ? (a, b) -> (b.getOwner() != null ? b.getOwner() : "").compareToIgnoreCase(a.getOwner() != null ? a.getOwner() : "")
                    : (a, b) -> (a.getOwner() != null ? a.getOwner() : "").compareToIgnoreCase(b.getOwner() != null ? b.getOwner() : "");
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName())
                    : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        List<SchemaInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<TableInfo> listTableInfos(Long connectionId, String schema) {
        return oracleMetadataRepository.listTableInfos(connectionId, schema);
    }

    public Page<TableInfo> listTablesPaged(Long connectionId, String schema, int page, int size, String sort, String order) {
        List<TableInfo> all = new ArrayList<>(listTableInfos(connectionId, schema));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<TableInfo> comparator;
        if ("type".equalsIgnoreCase(sortBy)) {
            comparator = desc ? (a, b) -> (b.getType() != null ? b.getType() : "").compareToIgnoreCase(a.getType() != null ? a.getType() : "")
                    : (a, b) -> (a.getType() != null ? a.getType() : "").compareToIgnoreCase(b.getType() != null ? b.getType() : "");
        } else if ("size".equals(sortBy)) {
            comparator = desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk())
                    : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk());
        } else if ("rows".equals(sortBy)) {
            comparator = desc ? (a, b) -> Long.compare(b.getApproximateRowCount(), a.getApproximateRowCount())
                    : (a, b) -> Long.compare(a.getApproximateRowCount(), b.getApproximateRowCount());
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName())
                    : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        List<TableInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String schema, String sql, int offset, int limit,
                                             String sortBy, String sortOrder) {
        return executeQuery(connectionId, schema, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String schema, String sql, int offset, int limit,
                                             String sortBy, String sortOrder, String search) {
        if (search != null && !search.isBlank()) {
            return executeQueryWithSearch(connectionId, schema, sql, offset, limit, sortBy, sortOrder, search.trim());
        }
        return executeQuery(connectionId, schema, sql, offset, limit, sortBy, sortOrder, true);
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String schema, String sql, int offset, int limit,
                                             String sortBy, String sortOrder, boolean truncateCells) {
        if (oracleMetadataRepository.getConnection(connectionId, schema).isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        String pagedSql = wrapWithLimitOffset(sql.trim(), limit, offset, sortBy, sortOrder);
        Optional<QueryResultData> dataOpt;
        try {
            dataOpt = oracleMetadataRepository.executeQuery(connectionId, schema, pagedSql);
        } catch (RuntimeException e) {
            return Optional.of(QueryResult.error(e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
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

    private Optional<QueryResult> executeQueryWithSearch(Long connectionId, String schema, String sql, int offset, int limit,
                                                         String sortBy, String sortOrder, String searchTerm) {
        if (oracleMetadataRepository.getConnection(connectionId, schema).isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        String upper = trimmed.toUpperCase().stripLeading();
        if (!upper.startsWith("SELECT") || upper.startsWith("SELECT INTO")) {
            return executeQuery(connectionId, schema, sql, offset, limit, sortBy, sortOrder, true);
        }
        String innerWithOrder = buildWrappedQueryWithOrder(trimmed, sortBy, sortOrder);
        Optional<QueryResultData> metaOpt;
        try {
            metaOpt = oracleMetadataRepository.executeQuery(connectionId, schema, innerWithOrder + " OFFSET 0 ROWS FETCH NEXT 0 ROWS ONLY");
        } catch (RuntimeException e) {
            return Optional.of(QueryResult.error(e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
        if (metaOpt.isEmpty() || metaOpt.get().getColumns() == null || metaOpt.get().getColumns().isEmpty()) {
            return Optional.of(QueryResult.error("Connection not available"));
        }
        List<String> columns = metaOpt.get().getColumns();
        String concatExpr = columns.stream()
                .map(c -> "TO_CHAR(\"_sub\".\"" + c.replace("\"", "\"\"") + "\")")
                .reduce((a, b) -> a + " || ':' || " + b)
                .orElse("''");
        String orderByClause = buildOrderByClause(sortBy, sortOrder);
        String searchSql = "SELECT * FROM (" + innerWithOrder + ") _sub WHERE (" + concatExpr + ") LIKE ? " + orderByClause.trim() + " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
        String likePattern = "%" + escapeForLike(searchTerm) + "%";
        int maxLimit = Math.min(limit, queryRowsLimit);
        List<Object> params = List.of(likePattern, Math.max(0, offset), maxLimit);
        Optional<QueryResultData> dataOpt;
        try {
            dataOpt = oracleMetadataRepository.executeQuery(connectionId, schema, searchSql, params);
        } catch (RuntimeException e) {
            return Optional.of(QueryResult.error(e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
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
        return "SELECT * FROM (" + trimmed + ") inner_rowlimit " + buildOrderByClause(sortBy, sortOrder);
    }

    private String buildOrderByClause(String sortBy, String sortOrder) {
        if (sortBy != null && !sortBy.isBlank() && sortOrder != null && !sortOrder.isBlank()
                && ("asc".equalsIgnoreCase(sortOrder) || "desc".equalsIgnoreCase(sortOrder))) {
            String quotedCol = "\"" + sortBy.replace("\"", "\"\"") + "\"";
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
        return buildWrappedQueryWithOrder(trimmed, sortBy, sortOrder)
                + " OFFSET " + Math.max(0, offset) + " ROWS FETCH NEXT " + maxLimit + " ROWS ONLY";
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

    public Map<String, Object> getDetailRowWithRowid(Long connectionId, String schema, String sql,
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
        if (qualified.contains(".")) {
            String[] dotParts = qualified.split("\\.", 2);
            schemaName = dotParts[0].replace("\"", "").trim();
            tableName = dotParts[1].replace("\"", "").trim();
        } else {
            tableName = qualified.replace("\"", "").trim();
        }

        Map<String, String> columnTypes = (schemaName != null && tableName != null)
                ? oracleMetadataRepository.getColumnTypes(connectionId, schemaName, tableName)
                : Map.of();

        String orderByClause = buildOrderByClause(sortBy, sortOrder);
        String innerSelect = "SELECT t.*, t.rowid AS row_id FROM " + qualified + " t";
        String pagedSql = "SELECT * FROM (SELECT inner_q.*, ROW_NUMBER() OVER (" + orderByClause + ") rn FROM (" + innerSelect + ") inner_q) WHERE rn = " + (rowNum + 1);
        Optional<Map<String, Object>> rowOpt;
        try {
            rowOpt = oracleMetadataRepository.executeQuerySingleRow(connectionId, schema, pagedSql);
        } catch (RuntimeException e) {
            log.warn("getDetailRowWithRowid failed: connectionId={}, schema={}, rowNum={}, sql={}", connectionId, schema, rowNum, pagedSql, e);
            out.put("error", e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            return out;
        }

        if (rowOpt.isEmpty()) {
            log.warn("getDetailRowWithRowid: connection not available for connectionId={}, schema={}", connectionId, schema);
            out.put("error", "error.connectionNotAvailable");
            return out;
        }

        Map<String, Object> row = rowOpt.get();
        if (row.isEmpty()) {
            log.warn("getDetailRowWithRowid: no row at position {} for connectionId={}, schema={}", rowNum, connectionId, schema);
            out.put("error", "No row at this position.");
            return out;
        }

        List<Map<String, String>> detailRows = new ArrayList<>();
        String rowRowid = null;
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if ("ROW_ID".equalsIgnoreCase(e.getKey())) {
                rowRowid = e.getValue() != null ? e.getValue().toString() : null;
                continue;
            }
            if ("RN".equalsIgnoreCase(e.getKey())) {
                continue;
            }
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("name", e.getKey());
            entry.put("type", columnTypes.getOrDefault(e.getKey(), "unknown"));
            entry.put("value", e.getValue() != null ? e.getValue().toString() : "");
            detailRows.add(entry);
        }

        out.put("detailRows", detailRows);
        out.put("rowRowid", rowRowid != null ? rowRowid : "");

        return out;
    }

    public Optional<String> executeUpdateByRowid(Long connectionId, String schema, String qualifiedTable,
                                                 String rowid, Map<String, String> columnValues) {
        if (rowid == null || rowid.isBlank() || qualifiedTable == null || qualifiedTable.isBlank()) {
            return Optional.of("Missing rowid or table.");
        }

        if (columnValues == null || columnValues.isEmpty()) {
            return Optional.empty();
        }

        String schemaName = null;
        String tableName = qualifiedTable.replace("\"", "").trim();
        if (qualifiedTable.contains(".")) {
            String[] dotParts = qualifiedTable.split("\\.", 2);
            schemaName = dotParts[0].replace("\"", "").trim();
            tableName = dotParts[1].replace("\"", "").trim();
        }

        Map<String, String> columnTypes = (schemaName != null && tableName != null)
                ? oracleMetadataRepository.getColumnTypes(connectionId, schemaName, tableName)
                : Map.of();
        List<String> setParts = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (Map.Entry<String, String> e : columnValues.entrySet()) {
            if ("ROW_ID".equalsIgnoreCase(e.getKey()) || "ROWID".equalsIgnoreCase(e.getKey())) {
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

        String qualified = (schemaName != null && !schemaName.isBlank())
                ? "\"" + schemaName.replace("\"", "\"\"") + "\".\"" + tableName.replace("\"", "\"\"") + "\""
                : "\"" + tableName.replace("\"", "\"\"") + "\"";
        String updateSql = "UPDATE " + qualified + " SET " + String.join(", ", setParts) + " WHERE rowid = ?";
        values.add(rowid.trim());

        return oracleMetadataRepository.executeUpdate(connectionId, schema, updateSql, values);
    }

    private static String sqlCastForDataType(String dataType) {
        if (dataType == null) {
            return "?";
        }
        return switch (dataType) {
            case "NUMBER" -> "TO_NUMBER(?)";
            case "FLOAT", "BINARY_FLOAT" -> "TO_BINARY_FLOAT(?)";
            case "BINARY_DOUBLE" -> "TO_BINARY_DOUBLE(?)";
            case "DATE" -> "TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')";
            case "TIMESTAMP" -> "TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF')";
            default -> "?";
        };
    }

    private static boolean isScalarDataType(String dataType) {
        if (dataType == null) {
            return false;
        }
        return switch (dataType) {
            case "NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "DATE", "TIMESTAMP" -> true;
            default -> false;
        };
    }
}
