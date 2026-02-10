package com.panopticum.core.model;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Introspected
public class SchemaInfo {

    private String name;
    private String owner;
    private int tableCount;
}
