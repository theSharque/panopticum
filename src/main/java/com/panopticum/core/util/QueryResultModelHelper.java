package com.panopticum.core.util;

import com.panopticum.core.error.ErrorKeys;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.ui.AppAlerts;
import lombok.experimental.UtilityClass;

import java.util.List;
import java.util.Map;

@UtilityClass
public class QueryResultModelHelper {

    public void putEmptyQueryPage(Map<String, Object> model, int limit) {
        AppAlerts.clear(model);
        model.put("columns", List.<String>of());
        model.put("columnTypes", List.<String>of());
        model.put("rows", List.<List<Object>>of());
        model.put("offset", 0);
        model.put("limit", limit);
        model.put("hasPrev", false);
        model.put("hasMore", false);
        model.put("nextOffset", limit);
        model.put("prevOffset", 0);
        model.put("fromRow", 0);
        model.put("toRow", 0);
        model.put("sort", "");
        model.put("order", "");
    }

    public void putQueryResult(Map<String, Object> model, QueryResult result, String sort, String order) {
        AppAlerts.fromControllerMessage(model, result.hasError() ? result.getError() : null);
        model.put("columns", result.getColumns());
        model.put("columnTypes", result.getColumnTypes() != null ? result.getColumnTypes() : List.<String>of());
        model.put("rows", result.getRows());
        model.put("offset", result.getOffset());
        model.put("limit", result.getLimit());
        model.put("hasPrev", result.hasPrev());
        model.put("hasMore", result.isHasMore());
        model.put("nextOffset", result.nextOffset());
        model.put("prevOffset", result.prevOffset());
        model.put("fromRow", result.fromRow());
        model.put("toRow", result.toRow());
        model.put("sort", sort != null ? sort : "");
        model.put("order", order != null ? order : "");
        if (result.getDocIds() != null) {
            model.put("docIds", result.getDocIds());
        }
        if (!model.containsKey("searchTerm")) {
            model.put("searchTerm", "");
        }
    }

    public void putEmptyQueryHtmxAlert(Map<String, Object> model) {
        AppAlerts.i18n(model, ErrorKeys.EMPTY_QUERY);
    }
}
