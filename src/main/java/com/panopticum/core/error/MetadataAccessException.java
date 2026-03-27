package com.panopticum.core.error;

public class MetadataAccessException extends RuntimeException {

    public MetadataAccessException(String message) {
        super(message);
    }

    public MetadataAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
