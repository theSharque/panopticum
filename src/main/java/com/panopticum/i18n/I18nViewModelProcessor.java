package com.panopticum.i18n;

import io.micronaut.http.HttpRequest;
import io.micronaut.views.ModelAndView;
import io.micronaut.views.model.ViewModelProcessor;
import jakarta.inject.Singleton;

import java.util.Map;

@Singleton
public class I18nViewModelProcessor implements ViewModelProcessor<Object> {

    @Override
    public void process(HttpRequest<?> request, ModelAndView<Object> modelAndView) {
        String locale = (String) request.getAttribute(LocaleFilter.LOCALE_ATTR).orElse("en");
        Map<String, String> msg = Messages.forLocale(locale);
        modelAndView.getModel().ifPresent(m -> {
            if (m instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> model = (Map<String, Object>) m;
                model.put("msg", msg);
                model.put("locale", locale);
                model.put("requestUri", request.getPath());
            }
        });
    }
}
