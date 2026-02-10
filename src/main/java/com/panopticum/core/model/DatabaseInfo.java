package com.panopticum.core.model;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Introspected
public class DatabaseInfo {

    private String name;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
