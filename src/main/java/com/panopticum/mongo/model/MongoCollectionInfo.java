package com.panopticum.mongo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MongoCollectionInfo {

    private String name;
    private long documentCount;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
