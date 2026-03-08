package com.panopticum.core.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Serdeable
public class TableInfo {

    private String name;
    private String type;
    private long approximateRowCount;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
