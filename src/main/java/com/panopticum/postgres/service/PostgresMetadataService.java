package com.panopticum.postgres.service;

import com.panopticum.core.error.ErrorKeys;
import com.panopticum.core.error.ServiceQueryErrors;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.sql.SqlPagingSupport;
import com.panopticum.core.sql.SqlQuerySupport;
import com.panopticum.core.util.QueryResultMapper;
import com.panopticum.core.model.EntityDescription;
import com.panopticum.postgres.PostgresJdbcDrivers;
import com.panopticum.postgres.PostgresWireCompat;
import com.panopticum.postgres.repository.PostgresMetadataRepository;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class PostgresMetadataService {

    private static final String POSTGRESQL_PREFIX = "jdbc:postgresql://";

    private final PostgresMetadataRepository postgresMetadataRepository;
    private final DbConnectionService dbConnectionService;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    @Value("${panopticum.limits.tables:1000}")
    private int tablesLimit;

    public Optional<Connection> getConnection(Long connectionId) {
        return postgresMetadataRepository.getConnection(connectionId);
    }

    public Optional<Connection> getConnection(Long connectionId, String dbName) {
        return postgresMetadataRepository.getConnection(connectionId, dbName);
    }

    public Optional<String> testConnection(String host, int port, String dbName, String username, String password) {
        if (host == null || host.isBlank() || dbName == null || dbName.isBlank() || username == null || username.isBlank()) {
            return Optional.of("error.specifyHostDbUser");
        }
        PostgresJdbcDrivers.ensureLoaded();
        String url = POSTGRESQL_PREFIX + host.trim() + ":" + port + "/" + dbName.trim();
        try (Connection c = DriverManager.getConnection(url, username.trim(), password != null ? password : "")) {
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public List<String> listDatabases(Long connectionId) {
        return postgresMetadataRepository.listDatabaseInfos(connectionId).stream().map(DatabaseInfo::getName).toList();
    }

    public List<DatabaseInfo> listDatabaseInfos(Long connectionId) {
        return postgresMetadataRepository.listDatabaseInfos(connectionId);
    }

    public Page<DatabaseInfo> listDatabasesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<DatabaseInfo> all = new ArrayList<>(listDatabaseInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        Comparator<DatabaseInfo> comparator = "size".equals(sortBy)
                ? (desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk()))
                : (desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        List<DatabaseInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<String> listSchemas(Long connectionId, String dbName, int offset, int limit) {
        return postgresMetadataRepository.listSchemaInfos(connectionId, dbName).stream().map(SchemaInfo::getName).toList();
    }

    public List<SchemaInfo> listSchemaInfos(Long connectionId, String dbName) {
        return postgresMetadataRepository.listSchemaInfos(connectionId, dbName);
    }

    public Page<SchemaInfo> listSchemasPaged(Long connectionId, String dbName, int page, int size, String sort, String order) {
        List<SchemaInfo> all = new ArrayList<>(listSchemaInfos(connectionId, dbName));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        Comparator<SchemaInfo> comparator;
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

    public List<TableInfo> listTables(Long connectionId, String dbName, String schema, int offset, int limit) {
        List<TableInfo> all = postgresMetadataRepository.listTableInfos(connectionId, dbName, schema);
        int end = Math.min(offset + Math.min(limit, tablesLimit - offset), all.size());
        return offset < all.size() ? all.subList(offset, end) : List.of();
    }

    public List<TableInfo> listTableInfos(Long connectionId, String dbName, String schema) {
        return postgresMetadataRepository.listTableInfos(connectionId, dbName, schema);
    }

    public Page<TableInfo> listTablesPaged(Long connectionId, String dbName, String schema, int page, int size, String sort, String order) {
        List<TableInfo> all = new ArrayList<>(listTableInfos(connectionId, dbName, schema));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        Comparator<TableInfo> comparator;
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
        return SqlQuerySupport.run(() -> executeQueryUnchecked(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, truncateCells));
    }

    private Optional<QueryResult> executeQueryUnchecked(Long connectionId, String dbName, String sql, int offset, int limit,
                                                        String sortBy, String sortOrder, boolean truncateCells) {
        if (postgresMetadataRepository.getConnection(connectionId, dbName).isEmpty()) {
            return ServiceQueryErrors.connectionUnavailable();
        }
        String pagedSql = wrapWithLimitOffset(sql.trim(), limit, offset, sortBy, sortOrder);
        Optional<QueryResultData> dataOpt = postgresMetadataRepository.executeQuery(connectionId, dbName, pagedSql);
        if (dataOpt.isEmpty()) {
            return ServiceQueryErrors.connectionUnavailable();
        }
        return Optional.of(QueryResultMapper.fromPaged(dataOpt.get(), offset, limit, truncateCells));
    }

    private Optional<QueryResult> executeQueryWithSearch(Long connectionId, String dbName, String sql, int offset, int limit,
                                                         String sortBy, String sortOrder, String searchTerm) {
        if (postgresMetadataRepository.getConnection(connectionId, dbName).isEmpty()) {
            return ServiceQueryErrors.connectionUnavailable();
        }
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        String upper = trimmed.toUpperCase().stripLeading();
        if (!upper.startsWith("SELECT") || upper.startsWith("SELECT INTO")) {
            return executeQuery(connectionId, dbName, sql, offset, limit, sortBy, sortOrder, true);
        }
        String inner = SqlPagingSupport.wrapForSubquery(trimmed, sortBy, sortOrder, PostgresMetadataService::quoteColumn,
                SqlPagingSupport.Style.LIMIT_OFFSET);
        Optional<QueryResultData> metaOpt = postgresMetadataRepository.executeQuery(connectionId, dbName, inner + " LIMIT 0");
        if (metaOpt.isEmpty() || metaOpt.get().getColumns() == null || metaOpt.get().getColumns().isEmpty()) {
            return ServiceQueryErrors.connectionUnavailable();
        }
        List<String> columns = metaOpt.get().getColumns();
        String concatExpr = columns.stream()
                .map(c -> "\"_sub\".\"" + c.replace("\"", "\"\"") + "\"::text")
                .reduce((a, b) -> a + " || ':' || " + b)
                .orElse("''");
        String orderByClause = SqlPagingSupport.orderByClause(sortBy, sortOrder, PostgresMetadataService::quoteColumn,
                SqlPagingSupport.Style.LIMIT_OFFSET);
        String searchSql = "SELECT * FROM (" + inner + ") AS _sub WHERE (" + concatExpr + ") LIKE ?" + orderByClause + " LIMIT ? OFFSET ?";
        String likePattern = "%" + escapeForLike(searchTerm) + "%";
        int maxLimit = Math.min(limit, queryRowsLimit);
        List<Object> params = List.of(likePattern, maxLimit, Math.max(0, offset));
        Optional<QueryResultData> dataOpt = postgresMetadataRepository.executeQuery(connectionId, dbName, searchSql, params);
        if (dataOpt.isEmpty()) {
            return ServiceQueryErrors.connectionUnavailable();
        }
        return Optional.of(QueryResultMapper.fromPaged(dataOpt.get(), offset, limit, true));
    }

    private static String escapeForLike(String term) {
        if (term == null) {
            return "";
        }
        return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    private String wrapWithLimitOffset(String sql, int limit, int offset, String sortBy, String sortOrder) {
        int maxLimit = Math.min(limit, queryRowsLimit);
        return SqlPagingSupport.wrapPagedSelect(sql, maxLimit, offset, sortBy, sortOrder,
                SqlPagingSupport.Style.LIMIT_OFFSET, PostgresMetadataService::quoteColumn);
    }

    private static String quoteColumn(String column) {
        return "\"" + column.replace("\"", "\"\"") + "\"";
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
        Optional<DbConnection> connMeta = dbConnectionService.findById(connectionId);
        if (connMeta.isEmpty() || !PostgresWireCompat.supportsCtidUpdates(connMeta.get().getType())) {
            out.put("error", "pg.ctidUnsupported");
            return out;
        }

        Optional<String> tableRef = parseTableFromSql(sql);

        if (tableRef.isEmpty()) {
            out.put("error", ErrorKeys.TABLE_NOT_DETERMINED);
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
                ? postgresMetadataRepository.getColumnTypes(connectionId, dbName, schemaName, tableName)
                : Map.of();

        boolean greenplum = isGreenplum(connMeta.get().getType());
        String rowLocatorSelect = greenplum
                ? qualified + ".ctid, " + qualified + ".gp_segment_id"
                : qualified + ".ctid";
        String modifiedSql = sql.strip().replaceFirst(";+\\s*$", "")
                .replaceFirst("(?i)SELECT\\s+\\*", "SELECT *, " + rowLocatorSelect);
        String pagedSql = wrapWithLimitOffset(modifiedSql, 1, rowNum, sortBy, sortOrder);
        Optional<Map<String, Object>> rowOpt = postgresMetadataRepository.executeQuerySingleRow(connectionId, dbName, pagedSql);

        if (rowOpt.isEmpty()) {
            out.put("error", ErrorKeys.CONNECTION_NOT_AVAILABLE);
            return out;
        }

        Map<String, Object> row = rowOpt.get();
        if (row.isEmpty()) {
            out.put("error", ErrorKeys.NO_ROW_AT_POSITION);
            return out;
        }

        List<Map<String, String>> detailRows = new ArrayList<>();
        String rowCtid = null;
        String rowGpSegmentId = null;
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if ("ctid".equalsIgnoreCase(e.getKey())) {
                rowCtid = e.getValue() != null ? e.getValue().toString() : null;
                continue;
            }
            if ("gp_segment_id".equalsIgnoreCase(e.getKey())) {
                rowGpSegmentId = e.getValue() != null ? e.getValue().toString() : null;
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
        out.put("rowGpSegmentId", rowGpSegmentId != null ? rowGpSegmentId : "");

        return out;
    }

    public Optional<String> executeUpdateByCtid(Long connectionId, String dbName, String qualifiedTable,
                                                String ctid, String gpSegmentId, Map<String, String> columnValues,
                                                Map<String, String> originalColumnValues) {
        if (ctid == null || ctid.isBlank() || qualifiedTable == null || qualifiedTable.isBlank()) {
            return Optional.of("Missing ctid or table.");
        }

        Optional<DbConnection> connMeta = dbConnectionService.findById(connectionId);
        if (connMeta.isEmpty() || !PostgresWireCompat.supportsCtidUpdates(connMeta.get().getType())) {
            return Optional.of("pg.ctidUnsupported");
        }
        boolean greenplum = isGreenplum(connMeta.get().getType());
        if (greenplum && (gpSegmentId == null || gpSegmentId.isBlank())) {
            return Optional.of("Missing gp_segment_id for Greenplum row update.");
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
                ? postgresMetadataRepository.getColumnTypes(connectionId, dbName, schemaName, tableName)
                : Map.of();
        List<String> setParts = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, String> e : columnValues.entrySet()) {
            if ("ctid".equalsIgnoreCase(e.getKey()) || "gp_segment_id".equalsIgnoreCase(e.getKey())) {
                continue;
            }
            String raw = e.getValue();
            if (originalColumnValues != null && originalColumnValues.containsKey(e.getKey())
                    && Objects.equals(raw, originalColumnValues.get(e.getKey()))) {
                continue;
            }
            String quoted = "\"" + e.getKey().replace("\"", "\"\"") + "\"";
            String dataType = columnTypes.get(e.getKey());
            String cast = sqlCastForDataType(dataType);
            setParts.add(quoted + " = " + cast);
            try {
                values.add(jdbcValueForDataType(e.getKey(), dataType, raw));
            } catch (IllegalArgumentException ex) {
                return Optional.of(ex.getMessage());
            }
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
        String where = " WHERE ctid = '" + trimmedCtid + "'::tid";
        if (greenplum) {
            String trimmedGpSegmentId = gpSegmentId.trim();
            if (!trimmedGpSegmentId.matches("-?\\d+")) {
                return Optional.of("Invalid gp_segment_id format.");
            }
            where += " AND gp_segment_id = ?";
            values.add(Integer.valueOf(trimmedGpSegmentId));
        }
        String updateSql = "UPDATE " + qualified + " SET " + String.join(", ", setParts) + where;

        return postgresMetadataRepository.executeUpdate(connectionId, dbName, updateSql, values);
    }

    private static boolean isGreenplum(String type) {
        return "greenplum".equalsIgnoreCase(type);
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

    private static Object jdbcValueForDataType(String columnName, String dataType, String raw) {
        if ((raw == null || raw.isBlank()) && isScalarDataType(dataType)) {
            return null;
        }
        if ("boolean".equals(dataType)) {
            return parseBoolean(raw)
                    .orElseThrow(() -> new IllegalArgumentException("Invalid boolean value for " + columnName + "."));
        }

        return raw != null ? raw : "";
    }

    private static Optional<Boolean> parseBoolean(String raw) {
        if (raw == null) {
            return Optional.empty();
        }

        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "true", "t", "1", "yes", "y", "on" -> Optional.of(true);
            case "false", "f", "0", "no", "n", "off" -> Optional.of(false);
            default -> Optional.empty();
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

    public Optional<EntityDescription> describeEntity(Long connectionId, String catalog, String namespace, String entity) {
        String db = catalog != null && !catalog.isBlank() ? catalog : "";
        String schema = namespace != null && !namespace.isBlank() ? namespace : "public";
        return postgresMetadataRepository.describeTable(connectionId, db, schema, entity);
    }
}
