package com.panopticum.clickhouse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChQueryResultData {

    private List<String> columns;
    private List<String> columnTypes;
    private List<List<Object>> rows;
}
