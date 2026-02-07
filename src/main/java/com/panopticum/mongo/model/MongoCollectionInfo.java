package com.panopticum.mongo.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MongoCollectionInfo {

    private String name;
    private long documentCount;
    private long sizeOnDisk;
    private String sizeOnDiskFormatted;
}
