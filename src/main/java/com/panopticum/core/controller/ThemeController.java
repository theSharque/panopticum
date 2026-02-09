package com.panopticum.core.controller;

import com.panopticum.i18n.RedirectHelper;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.cookie.Cookie;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

import java.net.URI;

@Controller("/theme")
@Secured(SecurityRule.IS_ANONYMOUS)
public class ThemeController {

    private static final String COOKIE_NAME = "theme";

    @Get
    public HttpResponse<?> setTheme(@QueryValue(defaultValue = "dark") String mode,
                                   @QueryValue(defaultValue = "/") String redirect) {
        String theme = "light".equalsIgnoreCase(mode) ? "light" : "dark";
        Cookie cookie = Cookie.of(COOKIE_NAME, theme).maxAge(365 * 24 * 60 * 60);
        String redirectTo = redirect != null && redirect.startsWith("/") ? redirect : "/";
        redirectTo = RedirectHelper.getRedirectForGet(redirectTo);
        return HttpResponse.seeOther(URI.create(redirectTo)).cookie(cookie);
    }
}
