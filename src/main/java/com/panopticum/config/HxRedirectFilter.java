package com.panopticum.config;

import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@Filter(Filter.MATCH_ALL_PATTERN)
public class HxRedirectFilter implements HttpServerFilter {

    public static final String HX_REDIRECT_ATTR = "hx.redirect";

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        return Flux.from(chain.proceed(request))
                .map(response -> {
                    request.getAttribute(HX_REDIRECT_ATTR, String.class).ifPresent(redirect ->
                            response.header("HX-Redirect", redirect));
                    return response;
                });
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
