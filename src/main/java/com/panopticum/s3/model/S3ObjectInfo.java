package com.panopticum.s3.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class S3ObjectInfo {

    private final String key;
    private final String size;
    private final long sizeBytes;
    private final String lastModified;
    private final String etag;
    private final boolean prefix;
}
