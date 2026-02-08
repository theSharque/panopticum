package com.panopticum.i18n;

import io.micronaut.http.HttpRequest;
import io.micronaut.views.ModelAndView;
import io.micronaut.views.model.ViewModelProcessor;
import jakarta.inject.Singleton;

import java.util.Map;

@Singleton
public class I18nViewModelProcessor implements ViewModelProcessor<Object> {

    private static final String THEME_COOKIE = "theme";
    private static final String DEFAULT_THEME = "dark";

    @Override
    public void process(HttpRequest<?> request, ModelAndView<Object> modelAndView) {
        String locale = (String) request.getAttribute(LocaleFilter.LOCALE_ATTR).orElse("en");
        String theme = request.getCookies().findCookie(THEME_COOKIE)
                .map(c -> c.getValue())
                .filter(v -> "light".equalsIgnoreCase(v) || "dark".equalsIgnoreCase(v))
                .orElse(DEFAULT_THEME);
        Map<String, String> msg = Messages.forLocale(locale);
        modelAndView.getModel().ifPresent(m -> {
            if (m instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> model = (Map<String, Object>) m;
                model.put("msg", msg);
                model.put("locale", locale);
                String path = request.getPath();
                String query = request.getUri().getRawQuery();
                String requestUri = query != null && !query.isEmpty() ? path + "?" + query : path;
                model.put("requestUri", requestUri);
                model.put("theme", theme);
            }
        });
    }
}
