package com.panopticum.mcp.model;

import com.panopticum.core.model.QueryResult;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@Serdeable
public class UnifiedQueryEnvelope {

    private SourceInfo source;
    private ScopeInfo scope;
    private QueryInfo query;
    private ResultInfo result;
    private RawInfo raw;
    private ErrorInfo error;

    @Data
    @Builder
    @Serdeable
    public static class SourceInfo {
        private Long connectionId;
        private String dbType;
        private String queryFormat;
    }

    @Data
    @Builder
    @Serdeable
    public static class ScopeInfo {
        private String catalog;
        private String namespace;
        private String entity;
    }

    @Data
    @Builder
    @Serdeable
    public static class QueryInfo {
        private String text;
        private String normalizedType;
    }

    @Data
    @Builder
    @Serdeable
    public static class ResultInfo {
        private List<ColumnInfo> columns;
        private List<Map<String, Object>> rows;
        private int rowCount;
        private boolean hasMore;
        private int offset;
        private int limit;
    }

    @Data
    @Builder
    @Serdeable
    public static class ColumnInfo {
        private String name;
        private String type;
    }

    @Data
    @Builder
    @Serdeable
    public static class RawInfo {
        private List<String> columns;
        private List<List<Object>> rows;
        private List<String> docIds;
    }

    @Data
    @Builder
    @Serdeable
    public static class ErrorInfo {
        private String code;
        private String message;
    }

    public static UnifiedQueryEnvelope fromQueryResult(QueryResult qr, Long connectionId, String dbType,
                                                      String queryFormat, String catalog, String namespace,
                                                      String entity, String queryText) {
        if (qr.hasError()) {
            return UnifiedQueryEnvelope.builder()
                    .source(SourceInfo.builder()
                            .connectionId(connectionId)
                            .dbType(dbType)
                            .queryFormat(queryFormat)
                            .build())
                    .scope(ScopeInfo.builder()
                            .catalog(catalog != null ? catalog : "")
                            .namespace(namespace != null ? namespace : "")
                            .entity(entity != null ? entity : "")
                            .build())
                    .query(QueryInfo.builder()
                            .text(queryText != null ? queryText : "")
                            .normalizedType(queryFormat)
                            .build())
                    .error(ErrorInfo.builder()
                            .message(qr.getError())
                            .build())
                    .build();
        }

        List<Map<String, Object>> rowsAsObjects = new ArrayList<>();
        List<String> columns = qr.getColumns();
        List<String> columnTypes = qr.getColumnTypes();
        List<String> docIds = qr.getDocIds();

        int rowIndex = 0;
        for (List<Object> row : qr.getRows()) {
            Map<String, Object> rowMap = new LinkedHashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                String col = columns.get(i);
                Object val = i < row.size() ? row.get(i) : null;
                rowMap.put(col, val);
            }
            if (docIds != null && rowIndex < docIds.size()) {
                rowMap.put("documentId", docIds.get(rowIndex));
            }
            rowsAsObjects.add(rowMap);
            rowIndex++;
        }

        List<ColumnInfo> colInfos = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            colInfos.add(ColumnInfo.builder()
                    .name(columns.get(i))
                    .type(columnTypes != null && i < columnTypes.size() ? columnTypes.get(i) : null)
                    .build());
        }

        return UnifiedQueryEnvelope.builder()
                .source(SourceInfo.builder()
                        .connectionId(connectionId)
                        .dbType(dbType)
                        .queryFormat(queryFormat)
                        .build())
                .scope(ScopeInfo.builder()
                        .catalog(catalog != null ? catalog : "")
                        .namespace(namespace != null ? namespace : "")
                        .entity(entity != null ? entity : "")
                        .build())
                .query(QueryInfo.builder()
                        .text(queryText != null ? queryText : "")
                        .normalizedType(queryFormat)
                        .build())
                .result(ResultInfo.builder()
                        .columns(colInfos)
                        .rows(rowsAsObjects)
                        .rowCount(qr.getRows().size())
                        .hasMore(qr.isHasMore())
                        .offset(qr.getOffset())
                        .limit(qr.getLimit())
                        .build())
                .raw(RawInfo.builder()
                        .columns(columns)
                        .rows(qr.getRows())
                        .docIds(docIds)
                        .build())
                .build();
    }
}
