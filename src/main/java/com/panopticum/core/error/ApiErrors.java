package com.panopticum.core.error;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ApiErrors {

    public static final String EMPTY_QUERY = "Empty query";

    public static final String QUERY_EXECUTION_FAILED = "Query execution failed.";

    public static final String TABLE_NOT_DETERMINED = "Could not determine table from SQL.";

    public static final String CONNECTION_NOT_AVAILABLE = "Connection not available.";

    public static final String NO_ROW_AT_POSITION = "No row at this position.";
}
