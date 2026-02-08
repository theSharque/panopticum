package com.panopticum.postgres.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo {

    private String name;
    private String type;
    private long approximateRowCount;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
