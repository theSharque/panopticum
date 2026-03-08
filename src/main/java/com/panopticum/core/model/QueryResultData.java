package com.panopticum.core.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Serdeable
public class QueryResultData {

    private List<String> columns;
    private List<String> columnTypes;
    private List<List<Object>> rows;
}
