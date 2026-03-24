package com.panopticum.elasticsearch.model;

public record ElasticsearchSearchResult(SearchResponseDto response, String failureMessage) {

    public static ElasticsearchSearchResult ok(SearchResponseDto response) {
        return new ElasticsearchSearchResult(response, null);
    }

    public static ElasticsearchSearchResult fail(String failureMessage) {
        return new ElasticsearchSearchResult(null, failureMessage);
    }
}
