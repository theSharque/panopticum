package com.panopticum.clickhouse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChDatabaseInfo {

    private String name;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
