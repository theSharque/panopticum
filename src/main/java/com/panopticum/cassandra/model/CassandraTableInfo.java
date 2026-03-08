package com.panopticum.cassandra.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class CassandraTableInfo {

    private String name;
    private String type;
    private String comment;
    private Integer defaultTimeToLive;
    private Integer gcGraceSeconds;
}
