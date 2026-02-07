package com.panopticum.i18n;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.cookie.Cookie;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

import java.net.URI;

@Controller("/locale")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class LocaleController {

    private static final String COOKIE_NAME = "locale";

    @Get
    public HttpResponse<?> setLocale(@QueryValue(defaultValue = "en") String lang,
                                     @QueryValue(defaultValue = "/") String redirect) {
        String locale = "ru".equalsIgnoreCase(lang) ? "ru" : "en";
        Cookie cookie = Cookie.of(COOKIE_NAME, locale).maxAge(365 * 24 * 60 * 60);
        String redirectTo = redirect.startsWith("/") ? redirect : "/";
        return HttpResponse.seeOther(URI.create(redirectTo)).cookie(cookie);
    }
}
