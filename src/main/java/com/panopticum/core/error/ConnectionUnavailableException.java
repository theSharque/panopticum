package com.panopticum.core.error;

public class ConnectionUnavailableException extends RuntimeException {

    public ConnectionUnavailableException() {
        super();
    }

    public ConnectionUnavailableException(Throwable cause) {
        super(cause);
    }
}
