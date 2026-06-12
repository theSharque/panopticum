package com.panopticum.core.sql;

import com.panopticum.core.error.ErrorKeys;
import com.panopticum.core.error.MetadataAccessException;
import com.panopticum.core.model.QueryResult;
import lombok.experimental.UtilityClass;

import java.util.Optional;
import java.util.function.Supplier;

@UtilityClass
public class SqlQuerySupport {

    public Optional<QueryResult> readOnlyBlock(boolean readOnly, String sql) {
        if (readOnly && sql != null && SqlStatementClassifier.isMutation(sql)) {
            return Optional.of(QueryResult.error(ErrorKeys.READ_ONLY_ENABLED));
        }

        return Optional.empty();
    }

    public Optional<QueryResult> run(Supplier<Optional<QueryResult>> action) {
        try {
            return action.get();
        } catch (MetadataAccessException e) {
            String message = e.getMessage() != null ? e.getMessage() : ErrorKeys.QUERY_EXECUTION_FAILED;

            return Optional.of(QueryResult.error(message));
        }
    }
}
