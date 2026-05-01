package com.panopticum.core.error;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class AccessResult<T> {

    public enum Status {
        OK, FORBIDDEN, UNAUTHORIZED, NOT_FOUND, UNSUPPORTED, ERROR
    }

    private final Status status;
    private final String messageKey;
    private final T payload;

    public static <T> AccessResult<T> ok(T payload) {
        return AccessResult.<T>builder().status(Status.OK).payload(payload).build();
    }

    public static <T> AccessResult<T> forbidden(String messageKey) {
        return AccessResult.<T>builder().status(Status.FORBIDDEN).messageKey(messageKey).build();
    }

    public static <T> AccessResult<T> unauthorized(String messageKey) {
        return AccessResult.<T>builder().status(Status.UNAUTHORIZED).messageKey(messageKey).build();
    }

    public static <T> AccessResult<T> notFound(String messageKey) {
        return AccessResult.<T>builder().status(Status.NOT_FOUND).messageKey(messageKey).build();
    }

    public static <T> AccessResult<T> unsupported() {
        return AccessResult.<T>builder().status(Status.UNSUPPORTED).messageKey("access.unsupported").build();
    }

    public static <T> AccessResult<T> error(String messageKey) {
        return AccessResult.<T>builder().status(Status.ERROR).messageKey(messageKey).build();
    }

    public boolean isOk() {
        return status == Status.OK;
    }
}
