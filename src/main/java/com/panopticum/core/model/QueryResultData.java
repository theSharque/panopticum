package com.panopticum.core.model;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Introspected
public class QueryResultData {

    private List<String> columns;
    private List<String> columnTypes;
    private List<List<Object>> rows;
}
