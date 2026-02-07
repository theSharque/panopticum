package com.panopticum.clickhouse.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ChTableInfo {

    private String name;
    private String type;
    private long approximateRowCount;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
