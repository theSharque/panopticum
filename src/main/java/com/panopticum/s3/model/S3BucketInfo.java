package com.panopticum.s3.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class S3BucketInfo {

    private final String name;
    private final String createdAt;
}
