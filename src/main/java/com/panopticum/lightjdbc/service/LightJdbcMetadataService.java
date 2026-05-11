package com.panopticum.lightjdbc.service;

import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.QueryResultData;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.StringUtils;
import com.panopticum.lightjdbc.repository.LightJdbcMetadataRepository;
import com.panopticum.mcp.model.EntityDescription;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class LightJdbcMetadataService {

    private static final Pattern FROM_TABLE = Pattern.compile("(?i)FROM\\s+([^\\s,;()]+)(?:\\s+[Aa][Ss]\\s+[^\\s,;()]+)?(?=[\\s,;(]|$)");

    private final LightJdbcMetadataRepository lightJdbcMetadataRepository;
    private final DbConnectionService dbConnectionService;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    public Optional<String> testConnection(String host, int port, String database, String username, String password, String type) {
        if (host == null || host.isBlank() || database == null || database.isBlank()) {
            return Optional.of("error.specifyHostDbUser");
        }
        String t = type != null ? type.toLowerCase() : "h2";
        if (!LightJdbcMetadataRepository.isLightJdbcType(t)) {
            return Optional.of("error.specifyHostDbUser");
        }
        DbConnection probe = DbConnection.builder()
                .type(t)
                .host(host.trim())
                .port(port > 0 ? port : LightJdbcMetadataRepository.defaultPort(t))
                .dbName(database.trim())
                .username(username != null ? username : "")
                .password(password != null ? password : "")
                .build();
        String url = LightJdbcMetadataRepository.buildUrl(probe);
        try (var c = DriverManager.getConnection(url, probe.getUsername(), probe.getPassword() != null ? probe.getPassword() : "")) {
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public String pseudoCatalog(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .map(c -> c.getDbName() != null && !c.getDbName().isBlank() ? c.getDbName() : "default")
                .orElse("default");
    }

    public List<DatabaseInfo> listPseudoCatalogInfos(Long connectionId) {
        String name = pseudoCatalog(connectionId);
        return List.of(new DatabaseInfo(name, 0L, ""));
    }

    public Page<DatabaseInfo> listDatabasesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<DatabaseInfo> all = new ArrayList<>(listPseudoCatalogInfos(connectionId));
        return Page.of(all, page, size, sort != null ? sort : "name", order != null ? order : "asc");
    }

    public List<SchemaInfo> listSchemaInfos(Long connectionId) {
        return lightJdbcMetadataRepository.listSchemaInfos(connectionId);
    }

    public Page<SchemaInfo> listSchemasPaged(Long connectionId, int page, int size, String sort, String order) {
        List<SchemaInfo> all = new ArrayList<>(listSchemaInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        Comparator<SchemaInfo> comparator = desc
                ? (a, b) -> b.getName().compareToIgnoreCase(a.getName())
                : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        List<SchemaInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<TableInfo> listTableInfos(Long connectionId, String schema) {
        return lightJdbcMetadataRepository.listTableInfos(connectionId, schema);
    }

    public Page<TableInfo> listTablesPaged(Long connectionId, String schema, int page, int size, String sort, String order) {
        List<TableInfo> all = new ArrayList<>(listTableInfos(connectionId, schema));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        Comparator<TableInfo> comparator;
        if ("type".equalsIgnoreCase(sortBy)) {
            comparator = desc
                    ? (a, b) -> (b.getType() != null ? b.getType() : "").compareToIgnoreCase(a.getType() != null ? a.getType() : "")
                    : (a, b) -> (a.getType() != null ? a.getType() : "").compareToIgnoreCase(b.getType() != null ? b.getType() : "");
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : Comparator.comparing(TableInfo::getName, String.CASE_INSENSITIVE_ORDER);
        }
        List<TableInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String sql, int offset, int limit, String sortBy, String sortOrder) {
        return executeQuery(connectionId, sql, offset, limit, sortBy, sortOrder, "");
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String sql, int offset, int limit,
                                             String sortBy, String sortOrder, String search) {
        if (search != null && !search.isBlank()) {
            return executeQueryWithSearch(connectionId, sql, offset, limit, sortBy, sortOrder, search.trim());
        }
        if (lightJdbcMetadataRepository.getConnection(connectionId).isEmpty()) {
            return Optional.of(QueryResult.error("error.connectionNotAvailable"));
        }
        int lim = Math.min(limit > 0 ? limit : 100, queryRowsLimit);
        int off = Math.max(0, offset);
        Optional<QueryResultData> dataOpt = lightJdbcMetadataRepository.executeQueryWindow(connectionId, sql.strip().replaceFirst(";+\\s*$", ""), off, lim);
        if (dataOpt.isEmpty()) {
            return Optional.of(QueryResult.error("error.queryExecutionFailed"));
        }
        QueryResultData data = dataOpt.get();
        boolean hasMore = data.getRows().size() == lim;
        List<List<Object>> rows = new ArrayList<>();
        for (List<Object> row : data.getRows()) {
            List<Object> t = new ArrayList<>();
            for (Object cell : row) {
                t.add(StringUtils.truncateCell(cell));
            }
            rows.add(t);
        }
        return Optional.of(new QueryResult(data.getColumns(), data.getColumnTypes(), rows, null, null, off, lim, hasMore));
    }

    private Optional<QueryResult> executeQueryWithSearch(Long connectionId, String sql, int offset, int limit,
                                                        String sortBy, String sortOrder, String searchTerm) {
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        String upper = trimmed.toUpperCase().stripLeading();
        if (!upper.startsWith("SELECT")) {
            return executeQuery(connectionId, sql, offset, limit, sortBy, sortOrder, "");
        }
        String whereClause = buildSearchPredicate(searchTerm, lightJdbcMetadataRepository.executeQueryWindow(connectionId, trimmed, 0, 1));
        if (whereClause == null) {
            return executeQuery(connectionId, sql, offset, limit, sortBy, sortOrder, "");
        }
        String wrapped = "SELECT * FROM (" + trimmed + ") __panopticum_sub WHERE " + whereClause;
        return executeQuery(connectionId, wrapped, offset, limit, sortBy, sortOrder, "");
    }

    private static String buildSearchPredicate(String searchTerm, Optional<QueryResultData> sampleOpt) {
        if (sampleOpt.isEmpty() || sampleOpt.get().getColumns().isEmpty()) {
            return null;
        }
        String term = searchTerm.replace("'", "''").toLowerCase();
        List<String> parts = new ArrayList<>();
        for (String col : sampleOpt.get().getColumns()) {
            parts.add("LOWER(CAST(" + col + " AS VARCHAR(32672))) LIKE '%" + term + "%'");
        }
        return String.join(" OR ", parts);
    }

    public Optional<EntityDescription> describeEntity(Long connectionId, String catalog, String namespace, String entity) {
        String schema = namespace != null && !namespace.isBlank() ? namespace : "";
        if (schema.isBlank()) {
            return Optional.empty();
        }
        return lightJdbcMetadataRepository.describeTable(connectionId, schema, entity);
    }

    public Map<String, Object> getDetailRow(Long connectionId, String schema, String sql, int rowNum, String sortBy, String sortOrder) {
        Map<String, Object> out = new LinkedHashMap<>();
        Optional<String> tableRef = parseTableFromSql(sql);

        if (tableRef.isEmpty()) {
            out.put("error", "Could not determine table from SQL.");
            out.put("editable", false);
            out.put("detailRows", List.<Map<String, String>>of());
            return out;
        }

        String qualified = tableRef.get();
        String tableName = parseTableName(qualified, schema);

        List<String> pkCols = lightJdbcMetadataRepository.getPrimaryKeyColumns(connectionId, schema, tableName);
        boolean editable = false;

        Map<String, String> columnTypes = lightJdbcMetadataRepository.getColumnTypes(connectionId, schema, tableName);

        String baseSql = sql.strip().replaceFirst(";+\\s*$", "");
        Optional<QueryResultData> win = lightJdbcMetadataRepository.executeQueryWindow(connectionId, baseSql, Math.max(0, rowNum), 1);
        if (win.isEmpty() || win.get().getRows().isEmpty()) {
            out.put("error", "error.connectionNotAvailable");
            out.put("editable", false);
            out.put("detailRows", List.<Map<String, String>>of());
            return out;
        }
        List<String> cols = win.get().getColumns();
        List<Object> row = win.get().getRows().get(0);
        Map<String, Object> rowMap = new LinkedHashMap<>();
        for (int i = 0; i < cols.size() && i < row.size(); i++) {
            rowMap.put(cols.get(i), row.get(i));
        }

        List<Map<String, String>> detailRows = new ArrayList<>();
        for (Map.Entry<String, Object> e : rowMap.entrySet()) {
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("name", e.getKey());
            entry.put("type", columnTypes.getOrDefault(e.getKey(), "unknown"));
            entry.put("value", e.getValue() != null ? e.getValue().toString() : "");
            entry.put("readOnly", "true");
            detailRows.add(entry);
        }

        out.put("detailRows", detailRows);
        out.put("editable", editable);
        out.put("uniqueKeyColumns", pkCols);
        out.put("uniqueKeyColumnsJoined", String.join(",", pkCols));
        out.put("qualifiedTable", qualified);

        return out;
    }

    private static String parseTableName(String qualified, String defaultSchema) {
        String q = qualified.replace("\"", "").trim();
        int dot = q.indexOf('.');
        if (dot > 0) {
            return q.substring(dot + 1).trim();
        }
        return q;
    }

    public Optional<String> parseTableFromSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return Optional.empty();
        }
        Matcher m = FROM_TABLE.matcher(sql);
        if (!m.find()) {
            return Optional.empty();
        }
        String ref = m.group(1).trim();
        int asIdx = ref.toUpperCase().indexOf(" AS ");
        String tableRef = asIdx >= 0 ? ref.substring(0, asIdx).trim() : ref;
        return tableRef.isBlank() ? Optional.empty() : Optional.of(tableRef);
    }
}
