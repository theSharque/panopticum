package com.panopticum.cassandra.service;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.util.StringUtils;
import com.panopticum.cassandra.model.CassandraKeyspaceInfo;
import com.panopticum.cassandra.model.CassandraQueryResultData;
import com.panopticum.cassandra.model.CassandraTableInfo;
import com.panopticum.cassandra.repository.CassandraMetadataRepository;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
public class CassandraMetadataService {

    private static final Pattern FROM_TABLE_CQL = Pattern.compile(
            "(?i)FROM\\s+\"([^\"]+)\"|FROM\\s+(\\w+)\\.(\\w+)|FROM\\s+(\\w+)(?=[\\s;]|$)");

    private final CassandraMetadataRepository cassandraMetadataRepository;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    public Optional<String> parseTableFromCql(String sql) {
        if (sql == null || sql.isBlank()) {
            return Optional.empty();
        }
        String trimmed = sql.strip().replaceFirst(";+\\s*$", "");
        Matcher m = FROM_TABLE_CQL.matcher(trimmed);
        if (!m.find()) {
            return Optional.empty();
        }
        if (m.group(1) != null) {
            return Optional.of(m.group(1).trim());
        }
        if (m.group(2) != null && m.group(3) != null) {
            return Optional.of(m.group(3).trim());
        }
        if (m.group(4) != null) {
            return Optional.of(m.group(4).trim());
        }
        return Optional.empty();
    }

    public Map<String, Object> getDetailRow(Long connectionId, String keyspaceName, String sql, int rowNum,
                                            String sortBy, String order) {
        Map<String, Object> out = new LinkedHashMap<>();
        Optional<String> tableOpt = parseTableFromCql(sql);
        if (tableOpt.isEmpty()) {
            out.put("editable", false);
            out.put("detailRows", List.<Map<String, String>>of());
            return out;
        }
        String tableName = tableOpt.get();
        List<String> primaryKeyColumns = cassandraMetadataRepository.getPrimaryKeyColumns(connectionId, keyspaceName, tableName);
        Optional<QueryResult> resultOpt = executeQuery(connectionId, keyspaceName, sql, 0, rowNum + 1);
        if (resultOpt.isEmpty() || resultOpt.get().hasError() || resultOpt.get().getRows() == null
                || resultOpt.get().getRows().size() <= rowNum) {
            out.put("editable", false);
            out.put("detailRows", List.<Map<String, String>>of());
            return out;
        }
        QueryResult result = resultOpt.get();
        List<String> columns = result.getColumns();
        List<String> types = result.getColumnTypes() != null ? result.getColumnTypes() : List.of();
        List<Object> row = result.getRows().get(rowNum);
        Set<String> keySet = Set.copyOf(primaryKeyColumns);
        List<Map<String, String>> detailRows = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String col = columns.get(i);
            String type = i < types.size() ? types.get(i) : "unknown";
            Object val = i < row.size() ? row.get(i) : null;
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("name", col);
            entry.put("type", type);
            entry.put("value", val != null ? val.toString() : "");
            entry.put("readOnly", String.valueOf(keySet.contains(col)));
            detailRows.add(entry);
        }
        boolean hasNonKeyColumn = columns.stream().anyMatch(c -> !keySet.contains(c));
        boolean editable = !primaryKeyColumns.isEmpty() && hasNonKeyColumn;
        out.put("detailRows", detailRows);
        out.put("editable", editable);
        out.put("uniqueKeyColumns", primaryKeyColumns);
        out.put("uniqueKeyColumnsJoined", String.join(",", primaryKeyColumns));
        out.put("qualifiedTable", keyspaceName + "." + tableName);
        return out;
    }

    public Optional<String> executeUpdateByKey(Long connectionId, String keyspaceName, String tableName,
                                              List<String> primaryKeyColumns,
                                              Map<String, Object> keyValues,
                                              Map<String, String> columnValues) {
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            return Optional.of("No primary key.");
        }
        if (tableName == null || tableName.isBlank()) {
            return Optional.of("Invalid table.");
        }
        Set<String> keySet = Set.copyOf(primaryKeyColumns);
        for (String pk : primaryKeyColumns) {
            if (keyValues == null || !keyValues.containsKey(pk) || keyValues.get(pk) == null) {
                return Optional.of("Missing key value for " + pk);
            }
        }
        Map<String, String> toUpdate = columnValues != null ? new LinkedHashMap<>(columnValues) : new LinkedHashMap<>();
        toUpdate.keySet().removeIf(keySet::contains);
        if (toUpdate.isEmpty()) {
            return Optional.empty();
        }
        Map<String, String> columnTypes = cassandraMetadataRepository.getColumnTypes(connectionId, keyspaceName, tableName);
        return cassandraMetadataRepository.executeUpdate(connectionId, keyspaceName, tableName,
                primaryKeyColumns, keyValues, toUpdate, columnTypes);
    }

    public Optional<String> testConnection(String host, int port, String keyspace, String username, String password) {
        if (host == null || host.isBlank()) {
            return Optional.of("error.specifyHost");
        }
        Optional<com.datastax.oss.driver.api.core.CqlSession> sessionOpt =
                cassandraMetadataRepository.createSessionForTest(host, port, keyspace, username, password);
        if (sessionOpt.isEmpty()) {
            return Optional.of("error.connectionNotAvailable");
        }
        try (com.datastax.oss.driver.api.core.CqlSession session = sessionOpt.get()) {
            session.execute(SimpleStatement.newInstance("SELECT * FROM system.local LIMIT 1"));
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage() != null ? e.getMessage() : "error.queryExecutionFailed");
        }
    }

    public List<CassandraKeyspaceInfo> listKeyspaceInfos(Long connectionId) {
        return cassandraMetadataRepository.listKeyspaceInfos(connectionId);
    }

    public Page<CassandraKeyspaceInfo> listKeyspacesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<CassandraKeyspaceInfo> all = new ArrayList<>(listKeyspaceInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<CassandraKeyspaceInfo> comparator;
        if ("durableWrites".equals(sortBy)) {
            comparator = desc
                    ? (a, b) -> Boolean.compare(b.getDurableWrites() != null && b.getDurableWrites(), a.getDurableWrites() != null && a.getDurableWrites())
                    : (a, b) -> Boolean.compare(a.getDurableWrites() != null && a.getDurableWrites(), b.getDurableWrites() != null && b.getDurableWrites());
        } else if ("replication".equals(sortBy)) {
            comparator = desc
                    ? (a, b) -> (b.getReplicationFormatted() != null ? b.getReplicationFormatted() : "").compareToIgnoreCase(a.getReplicationFormatted() != null ? a.getReplicationFormatted() : "")
                    : (a, b) -> (a.getReplicationFormatted() != null ? a.getReplicationFormatted() : "").compareToIgnoreCase(b.getReplicationFormatted() != null ? b.getReplicationFormatted() : "");
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        List<CassandraKeyspaceInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<CassandraTableInfo> listTableInfos(Long connectionId, String keyspaceName) {
        return cassandraMetadataRepository.listTableInfos(connectionId, keyspaceName);
    }

    public Page<CassandraTableInfo> listTablesPaged(Long connectionId, String keyspaceName, int page, int size, String sort, String order) {
        List<CassandraTableInfo> all = new ArrayList<>(listTableInfos(connectionId, keyspaceName));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<CassandraTableInfo> comparator;
        if ("type".equalsIgnoreCase(sortBy)) {
            comparator = desc ? (a, b) -> (b.getType() != null ? b.getType() : "").compareToIgnoreCase(a.getType() != null ? a.getType() : "")
                    : (a, b) -> (a.getType() != null ? a.getType() : "").compareToIgnoreCase(b.getType() != null ? b.getType() : "");
        } else if ("comment".equals(sortBy)) {
            comparator = desc ? (a, b) -> (b.getComment() != null ? b.getComment() : "").compareToIgnoreCase(a.getComment() != null ? a.getComment() : "")
                    : (a, b) -> (a.getComment() != null ? a.getComment() : "").compareToIgnoreCase(b.getComment() != null ? b.getComment() : "");
        } else if ("ttl".equals(sortBy)) {
            comparator = desc ? (a, b) -> Integer.compare(b.getDefaultTimeToLive() != null ? b.getDefaultTimeToLive() : 0, a.getDefaultTimeToLive() != null ? a.getDefaultTimeToLive() : 0)
                    : (a, b) -> Integer.compare(a.getDefaultTimeToLive() != null ? a.getDefaultTimeToLive() : 0, b.getDefaultTimeToLive() != null ? b.getDefaultTimeToLive() : 0);
        } else if ("gcGrace".equals(sortBy)) {
            comparator = desc ? (a, b) -> Integer.compare(b.getGcGraceSeconds() != null ? b.getGcGraceSeconds() : 0, a.getGcGraceSeconds() != null ? a.getGcGraceSeconds() : 0)
                    : (a, b) -> Integer.compare(a.getGcGraceSeconds() != null ? a.getGcGraceSeconds() : 0, b.getGcGraceSeconds() != null ? b.getGcGraceSeconds() : 0);
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        List<CassandraTableInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String keyspaceName, String cql, int offset, int limit, boolean truncateCells) {
        int lim = Math.min(limit > 0 ? limit : 100, queryRowsLimit);
        Optional<CassandraQueryResultData> dataOpt = cassandraMetadataRepository.executeCql(connectionId, keyspaceName, cql, lim);
        if (dataOpt.isEmpty()) {
            return Optional.of(QueryResult.error("error.connectionNotAvailable"));
        }
        CassandraQueryResultData data = dataOpt.get();
        boolean hasMore = data.getRows() != null && data.getRows().size() == lim;
        List<List<Object>> rows = data.getRows() != null ? new ArrayList<>(data.getRows()) : List.of();
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
        return Optional.of(new QueryResult(
                data.getColumns() != null ? data.getColumns() : List.of(),
                data.getColumnTypes() != null ? data.getColumnTypes() : List.of(),
                rows,
                null, null,
                offset, lim, hasMore));
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String keyspaceName, String cql, int offset, int limit) {
        return executeQuery(connectionId, keyspaceName, cql, offset, limit, true);
    }
}
