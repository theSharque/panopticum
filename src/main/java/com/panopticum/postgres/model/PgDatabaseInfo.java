package com.panopticum.postgres.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PgDatabaseInfo {

    private String name;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
