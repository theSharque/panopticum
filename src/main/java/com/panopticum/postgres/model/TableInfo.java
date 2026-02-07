package com.panopticum.postgres.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TableInfo {

    private final String name;
    private final String type;
}
