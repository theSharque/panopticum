package com.panopticum.postgres.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PgSchemaInfo {

    private String name;
    private String owner;
    private int tableCount;
}
