package com.panopticum.core.ui;

import lombok.Value;

@Value
public class AppAlert {

    String i18nKey;
    String rawText;

    public static AppAlert i18n(String key) {
        return new AppAlert(key, null);
    }

    public static AppAlert raw(String text) {
        return new AppAlert(null, text);
    }
}
