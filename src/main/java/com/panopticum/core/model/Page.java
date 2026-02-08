package com.panopticum.core.model;

import lombok.Getter;

import java.util.List;

@Getter
public class Page<T> {

    private final List<T> items;
    private final int page;
    private final int size;
    private final String sort;
    private final String order;
    private final int fromRow;
    private final int toRow;
    private final boolean hasPrev;
    private final boolean hasMore;
    private final int prevOffset;
    private final int nextOffset;

    public Page(List<T> items, int page, int size, String sort, String order,
                int fromRow, int toRow, boolean hasPrev, boolean hasMore,
                int prevOffset, int nextOffset) {
        this.items = items != null ? items : List.of();
        this.page = page;
        this.size = size;
        this.sort = sort != null ? sort : "name";
        this.order = order != null ? order : "asc";
        this.fromRow = fromRow;
        this.toRow = toRow;
        this.hasPrev = hasPrev;
        this.hasMore = hasMore;
        this.prevOffset = prevOffset;
        this.nextOffset = nextOffset;
    }

    public static <T> Page<T> of(List<T> sortedItems, int page, int size, String sort, String order) {
        int limit = Math.min(Math.max(1, size), 500);
        int offset = Math.max(0, (page - 1) * limit);
        List<T> pageItems = offset < sortedItems.size()
                ? sortedItems.subList(offset, Math.min(offset + limit, sortedItems.size()))
                : List.of();
        int from = sortedItems.isEmpty() ? 0 : offset + 1;
        int to = offset + pageItems.size();
        return new Page<>(pageItems, page, limit, sort, order,
                from, to, page > 1, offset + pageItems.size() < sortedItems.size(),
                Math.max(0, offset - limit), offset + limit);
    }
}
