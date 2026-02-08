package com.panopticum.postgres.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PgDatabaseInfo {

    private String name;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
