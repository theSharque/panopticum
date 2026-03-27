package com.panopticum.core.util;

import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.Page;
import com.panopticum.core.service.DbConnectionService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class ControllerModelHelper {

    private ControllerModelHelper() {
    }

    public static Map<String, Object> baseModel(Long id, DbConnectionService dbConnectionService) {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        dbConnectionService.findById(id).ifPresent(conn -> model.put("connection", conn));

        return model;
    }

    public static void addBreadcrumbs(Map<String, Object> model, List<BreadcrumbItem> breadcrumbs) {
        List<BreadcrumbItem> list = breadcrumbs != null ? breadcrumbs : List.of();
        model.put("breadcrumbs", list);
        putBreadcrumbPath(model, list);
    }

    public static void refreshBreadcrumbPath(Map<String, Object> model) {
        @SuppressWarnings("unchecked")
        List<BreadcrumbItem> list = (List<BreadcrumbItem>) model.get("breadcrumbs");
        putBreadcrumbPath(model, list != null ? list : List.of());
    }

    private static void putBreadcrumbPath(Map<String, Object> model, List<BreadcrumbItem> breadcrumbs) {
        model.put("breadcrumbPath", joinBreadcrumbLabels(breadcrumbs));
    }

    private static String joinBreadcrumbLabels(List<BreadcrumbItem> breadcrumbs) {
        if (breadcrumbs == null || breadcrumbs.isEmpty()) {
            return "";
        }
        return breadcrumbs.stream()
                .map(b -> b.getLabel() != null ? b.getLabel() : "")
                .collect(Collectors.joining("/"));
    }

    public static <T> void addPagination(Map<String, Object> model, Page<T> page, String itemsKey) {
        if (page == null) {
            return;
        }
        model.put(itemsKey != null ? itemsKey : "items", page.getItems());
        model.put("page", page.getPage());
        model.put("size", page.getSize());
        model.put("sort", page.getSort());
        model.put("order", page.getOrder());
        model.put("fromRow", page.getFromRow());
        model.put("toRow", page.getToRow());
        model.put("hasPrev", page.isHasPrev());
        model.put("hasMore", page.isHasMore());
        model.put("prevOffset", page.getPrevOffset());
        model.put("nextOffset", page.getNextOffset());
    }

    public static void addOrderToggles(Map<String, Object> model, String sort, String order,
                                       Map<String, String> sortColumnToOrderKey) {
        if (sort == null) {
            sort = "name";
        }
        if (order == null) {
            order = "asc";
        }
        String orderVal = order;
        String sortBy = sort;
        boolean isAsc = "asc".equalsIgnoreCase(orderVal);
        if (sortColumnToOrderKey != null) {
            for (Map.Entry<String, String> e : sortColumnToOrderKey.entrySet()) {
                String modelKey = e.getValue();
                String column = e.getKey();
                String toggle = sortBy.equals(column) && isAsc ? "desc" : "asc";
                model.put(modelKey, toggle);
            }
        }
    }
}
