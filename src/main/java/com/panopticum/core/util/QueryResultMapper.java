package com.panopticum.core.util;

import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.QueryResultData;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class QueryResultMapper {

    public static QueryResult fromPaged(QueryResultData data, int offset, int limit, boolean truncateCells) {
        boolean hasMore = data.getRows().size() == limit;
        List<List<Object>> rows = data.getRows().size() > limit
                ? data.getRows().subList(0, limit)
                : data.getRows();
        if (truncateCells) {
            rows = truncateRows(rows);
        }

        return new QueryResult(data.getColumns(), data.getColumnTypes(), rows, null, null, offset, limit, hasMore);
    }

    public static QueryResult fromRows(QueryResultData data, int offset, int limit, boolean truncateCells) {
        boolean hasMore = data.getRows().size() == limit;
        List<List<Object>> rows = data.getRows();
        if (truncateCells) {
            rows = truncateRows(rows);
        }

        return new QueryResult(data.getColumns(), data.getColumnTypes(), rows, null, null, offset, limit, hasMore);
    }

    private static List<List<Object>> truncateRows(List<List<Object>> rows) {
        List<List<Object>> truncated = new ArrayList<>();
        for (List<Object> row : rows) {
            List<Object> t = new ArrayList<>();
            for (Object cell : row) {
                t.add(StringUtils.truncateCell(cell));
            }
            truncated.add(t);
        }

        return truncated;
    }
}
