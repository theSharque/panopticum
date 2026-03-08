package com.panopticum.cassandra.model;

import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class CassandraKeyspaceInfo {

    private String name;
    private Boolean durableWrites;
    private String replicationFormatted;
}
