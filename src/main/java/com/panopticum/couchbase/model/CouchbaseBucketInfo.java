package com.panopticum.couchbase.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Serdeable
public class CouchbaseBucketInfo {

    private String name;
    private String bucketType;
    private long ramQuotaMb;
    private int replicaCount;
}
