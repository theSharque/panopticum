package com.panopticum.core.error;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ApiErrors {

    public static final String EMPTY_QUERY = "Empty query";

    public static final String QUERY_EXECUTION_FAILED = "Query execution failed.";

    public static final String TABLE_NOT_DETERMINED = "Could not determine table from SQL.";
}
