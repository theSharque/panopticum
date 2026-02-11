package com.panopticum.i18n;

import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import org.reactivestreams.Publisher;

@Filter(Filter.MATCH_ALL_PATTERN)
public class LocaleFilter implements HttpServerFilter {

    public static final String LOCALE_ATTR = "locale";
    private static final String COOKIE_NAME = "locale";
    private static final String DEFAULT_LOCALE = "en";

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        String locale = request.getCookies().findCookie(COOKIE_NAME)
                .map(c -> c.getValue())
                .filter(v -> !v.isBlank())
                .orElseGet(() -> resolveFromAcceptLanguage(request));
        request.setAttribute(LOCALE_ATTR, locale);
        return chain.proceed(request);
    }

    private String resolveFromAcceptLanguage(HttpRequest<?> request) {
        String header = request.getHeaders().get("Accept-Language");
        if (header == null || header.isBlank()) {
            return DEFAULT_LOCALE;
        }
        return parsePreferredLocale(header);
    }

    private String parsePreferredLocale(String acceptLanguage) {
        String first = acceptLanguage.split(",")[0].trim().toLowerCase();
        if (first.startsWith("ru")) {
            return "ru";
        }
        if (first.startsWith("en")) {
            return "en";
        }
        return DEFAULT_LOCALE;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
