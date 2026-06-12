package com.panopticum.core.util;

import com.panopticum.core.error.ErrorKeys;
import com.panopticum.core.model.ConnectionTestResponse;
import com.panopticum.core.service.ConnectionTestService;
import lombok.experimental.UtilityClass;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

@UtilityClass
public class ConnectionTestHelper {

    public ConnectionTestResponse toApiResponse(ConnectionTestService connectionTestService, String type,
                                                String host, Integer port, String database, String username,
                                                String password, Long connectionId, Boolean useHttps) {
        try {
            Optional<String> error = connectionTestService.test(type, host, port, database, username, password,
                    Optional.ofNullable(connectionId), useHttps);
            return new ConnectionTestResponse(error.isEmpty(), error.orElse("connectionTest.success"));
        } catch (Exception e) {
            String messageKey = e.getMessage() != null ? e.getMessage() : ErrorKeys.QUERY_EXECUTION_FAILED;
            return new ConnectionTestResponse(false, messageKey);
        }
    }

    public Map<String, Object> toUiModel(ConnectionTestService connectionTestService, String type,
                                           String host, Integer port, String database, String username,
                                           String password, Optional<Long> connectionId,
                                           Boolean couchbaseTlsOverride, Boolean useHttpsOverride) {
        Map<String, Object> model = new HashMap<>();
        try {
            Optional<String> error = connectionTestService.test(type, host, port, database, username, password,
                    connectionId, couchbaseTlsOverride, useHttpsOverride);
            model.put("success", error.isEmpty());
            String messageKey = error.orElse("connectionTest.success");
            model.put("message", messageKey);
        } catch (Exception e) {
            model.put("success", false);
            String messageKey = e.getMessage() != null ? e.getMessage() : ErrorKeys.QUERY_EXECUTION_FAILED;
            model.put("message", messageKey);
        }
        return model;
    }

    public Map<String, Object> toUiModel(ConnectionTestService connectionTestService, String type,
                                           String host, Integer port, String database, String username,
                                           String password, Optional<Long> connectionId, Boolean useHttpsOverride) {
        return toUiModel(connectionTestService, type, host, port, database, username, password, connectionId,
                null, useHttpsOverride);
    }

    public void applyDisplayText(Map<String, Object> model, BiConsumer<Map<String, Object>, String> displayTextSetter) {
        if (displayTextSetter != null && model.get("message") instanceof String messageKey) {
            displayTextSetter.accept(model, messageKey);
        }
    }
}
