package com.panopticum.core.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Serdeable
public class SchemaInfo {

    private String name;
    private String owner;
    private int tableCount;
}
