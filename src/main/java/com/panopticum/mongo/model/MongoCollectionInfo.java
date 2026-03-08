package com.panopticum.mongo.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class MongoCollectionInfo {

    private String name;
    private long documentCount;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
