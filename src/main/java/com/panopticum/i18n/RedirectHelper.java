package com.panopticum.i18n;

public final class RedirectHelper {

    private static final String POST_ONLY_SUFFIX = "/detail/update";

    private RedirectHelper() {
    }

    public static String getRedirectForGet(String path) {
        if (path == null || !path.endsWith(POST_ONLY_SUFFIX)) {
            return path;
        }
        String result = path.substring(0, path.length() - "/update".length());
        return result.isEmpty() ? "/" : result;
    }
}
