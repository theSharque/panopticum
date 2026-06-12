package com.panopticum.core.error;

import com.panopticum.core.model.QueryResult;
import lombok.experimental.UtilityClass;

import java.util.Optional;

@UtilityClass
public class ServiceQueryErrors {

    public static Optional<QueryResult> connectionUnavailable() {
        return Optional.of(QueryResult.error(ErrorKeys.CONNECTION_NOT_AVAILABLE));
    }
}
