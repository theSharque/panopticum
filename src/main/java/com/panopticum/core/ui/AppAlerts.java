package com.panopticum.core.ui;

import java.util.Map;
import java.util.regex.Pattern;

public final class AppAlerts {

    private static final Pattern MESSAGE_KEY =
            Pattern.compile("^[a-z][a-z0-9_]*(\\.[a-zA-Z][a-zA-Z0-9_]*)+$");

    private AppAlerts() {
    }

    public static void clear(Map<String, Object> model) {
        model.remove("appAlert");
    }

    public static void i18n(Map<String, Object> model, String key) {
        if (key == null || key.isBlank()) {
            clear(model);
            return;
        }
        model.put("appAlert", AppAlert.i18n(key));
    }

    public static void raw(Map<String, Object> model, String text) {
        if (text == null || text.isBlank()) {
            clear(model);
            return;
        }
        model.put("appAlert", AppAlert.raw(text));
    }

    public static void fromControllerMessage(Map<String, Object> model, String message) {
        if (message == null || message.isBlank()) {
            clear(model);
            return;
        }
        if (MESSAGE_KEY.matcher(message).matches()) {
            i18n(model, message);
        } else {
            raw(model, message);
        }
    }
}
