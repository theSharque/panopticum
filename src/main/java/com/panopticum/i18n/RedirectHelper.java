package com.panopticum.i18n;

import lombok.experimental.UtilityClass;

@UtilityClass
public class RedirectHelper {

    private static final String POST_ONLY_SUFFIX = "/detail/update";

    public String getRedirectForGet(String path) {
        if (path == null || !path.endsWith(POST_ONLY_SUFFIX)) {
            return path;
        }
        String result = path.substring(0, path.length() - "/update".length());
        return result.isEmpty() ? "/" : result;
    }
}
