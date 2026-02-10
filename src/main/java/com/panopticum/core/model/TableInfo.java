package com.panopticum.core.model;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Introspected
public class TableInfo {

    private String name;
    private String type;
    private long approximateRowCount;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
