package com.panopticum.core.ui;

import lombok.experimental.UtilityClass;

import java.util.Map;
import java.util.regex.Pattern;

@UtilityClass
public class AppAlerts {

    private static final Pattern MESSAGE_KEY =
            Pattern.compile("^[a-z][a-z0-9_]*(\\.[a-zA-Z][a-zA-Z0-9_]*)+$");

    public void clear(Map<String, Object> model) {
        model.remove("appAlert");
    }

    public void i18n(Map<String, Object> model, String key) {
        if (key == null || key.isBlank()) {
            clear(model);
            return;
        }
        model.put("appAlert", AppAlert.i18n(key));
    }

    public void raw(Map<String, Object> model, String text) {
        if (text == null || text.isBlank()) {
            clear(model);
            return;
        }
        model.put("appAlert", AppAlert.raw(text));
    }

    public void fromControllerMessage(Map<String, Object> model, String message) {
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
