package com.panopticum.clickhouse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChTableInfo {

    private String name;
    private String type;
    private long approximateRowCount;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
