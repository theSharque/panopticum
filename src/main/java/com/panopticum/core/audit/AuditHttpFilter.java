package com.panopticum.core.audit;

import com.panopticum.core.model.ConnectionType;
import com.panopticum.core.service.DbConnectionService;
import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import jakarta.inject.Inject;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

@Filter(Filter.MATCH_ALL_PATTERN)
public class AuditHttpFilter implements HttpServerFilter {

    private static final Pattern NUMERIC_SEGMENT = Pattern.compile("/\\d+");

    @Inject
    private AuditService auditService;

    @Inject
    private DbConnectionService dbConnectionService;

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        long startNanos = System.nanoTime();
        String method = request.getMethodName();
        String path = request.getPath();

        return Flux.from(chain.proceed(request))
                .doOnNext(response -> record(request, method, path, response.getStatus().getCode(), startNanos));
    }

    private void record(HttpRequest<?> request, String method, String path, int status, long startNanos) {
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000;

        if (HttpMethod.GET.equals(request.getMethod())) {
            AuditBrowseMatcher.match(method, path).ifPresent(match ->
                    auditService.browse(match.getConnectionId(), match.getConnectionType(),
                            match.getAction(), match.getScope()));
        }

        matchMutation(method, path).ifPresent(conn ->
                auditService.rowUpdate(conn.id(), conn.type()));

        if (HttpMethod.POST.equals(request.getMethod()) && isQueryPath(path)) {
            resolveConnectionFromPath(path).ifPresent(conn ->
                    auditService.query(conn.id(), conn.type(), null));
        }

        if (path != null && path.startsWith("/api/") && !path.startsWith("/api/connections")) {
            auditService.apiCall(method, normalizeRoute(path), status, durationMs);
        }
    }

    private Optional<ConnectionRef> matchMutation(String method, String path) {
        if (path == null || method == null) {
            return Optional.empty();
        }

        String upper = method.toUpperCase(Locale.ROOT);
        if ("POST".equals(upper) && path.endsWith("/detail/update")) {
            return resolveConnectionFromPath(path);
        }

        if ("PUT".equals(upper) && path.contains("/row")) {
            return resolveConnectionFromPath(path);
        }

        if ("PUT".equals(upper) && path.contains("/doc/")) {
            return resolveConnectionFromPath(path);
        }

        if ("POST".equals(upper) && path.contains("/document")) {
            return resolveConnectionFromPath(path);
        }

        return Optional.empty();
    }

    private boolean isQueryPath(String path) {
        if (path == null) {
            return false;
        }

        String lower = path.toLowerCase(Locale.ROOT);
        return lower.endsWith("/query")
                || lower.endsWith("/sql")
                || lower.endsWith("/cql");
    }

    private Optional<ConnectionRef> resolveConnectionFromPath(String path) {
        for (ConnectionType type : ConnectionType.values()) {
            Optional<ConnectionRef> ui = extractConnId(path, type.getUiPathPrefix());
            if (ui.isPresent()) {
                return ui.map(ref -> enrichType(ref, type.getCanonicalId()));
            }

            String apiPrefix = type.getApiPathPrefix();
            if (apiPrefix != null) {
                Optional<ConnectionRef> api = extractConnId(path, apiPrefix);
                if (api.isPresent()) {
                    return api.map(ref -> enrichType(ref, type.getCanonicalId()));
                }
            }
        }

        for (String prefix : new String[]{"/kubernetes", "/s3", "/prometheus"}) {
            Optional<ConnectionRef> ref = extractConnId(path, prefix);
            if (ref.isPresent()) {
                return ref.map(r -> enrichType(r, prefix.substring(1)));
            }
        }

        return Optional.empty();
    }

    private ConnectionRef enrichType(ConnectionRef ref, String type) {
        if (ref.type() != null && !ref.type().isBlank()) {
            return ref;
        }

        return dbConnectionService.findById(ref.id())
                .map(conn -> new ConnectionRef(ref.id(), conn.getType()))
                .orElse(new ConnectionRef(ref.id(), type));
    }

    private Optional<ConnectionRef> extractConnId(String path, String prefix) {
        if (!path.startsWith(prefix + "/")) {
            return Optional.empty();
        }

        String remainder = path.substring(prefix.length() + 1);
        int slash = remainder.indexOf('/');
        String idSegment = slash >= 0 ? remainder.substring(0, slash) : remainder;
        try {
            return Optional.of(new ConnectionRef(Long.parseLong(idSegment), null));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private String normalizeRoute(String path) {
        return NUMERIC_SEGMENT.matcher(path).replaceAll("/{id}");
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    private record ConnectionRef(Long id, String type) {
    }
}
