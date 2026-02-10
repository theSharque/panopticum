package com.panopticum.i18n;

import java.util.HashMap;
import java.util.Map;

public final class Messages {

    private Messages() {
    }

    public static Map<String, String> forLocale(String locale) {
        Map<String, String> merged = new HashMap<>();
        if ("ru".equalsIgnoreCase(locale)) {
            merged.putAll(MessagesCommon.RU);
            merged.putAll(MessagesSettings.RU);
            merged.putAll(MessagesPg.RU);
            merged.putAll(MessagesMongo.RU);
            merged.putAll(MessagesRedis.RU);
            merged.putAll(MessagesCh.RU);
            merged.putAll(MessagesCassandra.RU);
            merged.putAll(MessagesRabbitmq.RU);
        } else {
            merged.putAll(MessagesCommon.EN);
            merged.putAll(MessagesSettings.EN);
            merged.putAll(MessagesPg.EN);
            merged.putAll(MessagesMongo.EN);
            merged.putAll(MessagesRedis.EN);
            merged.putAll(MessagesCh.EN);
            merged.putAll(MessagesCassandra.EN);
            merged.putAll(MessagesRabbitmq.EN);
        }
        return merged;
    }
}
