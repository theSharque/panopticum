package com.panopticum.postgres.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PgSchemaInfo {

    private String name;
    private String owner;
    private int tableCount;
}
