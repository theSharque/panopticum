package com.panopticum.core.error;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ErrorKeys {

    public static final String EMPTY_QUERY = "error.emptyQuery";

    public static final String QUERY_EXECUTION_FAILED = "error.queryExecutionFailed";

    public static final String TABLE_NOT_DETERMINED = "error.tableNotDetermined";

    public static final String CONNECTION_NOT_AVAILABLE = "error.connectionNotAvailable";

    public static final String NO_ROW_AT_POSITION = "error.noRowAtPosition";

    public static final String READ_ONLY_ENABLED = "read.only.enabled";
}
