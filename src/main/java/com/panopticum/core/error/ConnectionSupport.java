package com.panopticum.core.error;

import java.util.Optional;

public final class ConnectionSupport {

    private ConnectionSupport() {
    }

    public static <T> T require(Optional<T> connection) {
        return connection.orElseThrow(ConnectionUnavailableException::new);
    }
}
