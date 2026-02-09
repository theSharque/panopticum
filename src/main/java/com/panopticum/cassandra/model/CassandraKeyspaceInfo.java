package com.panopticum.cassandra.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CassandraKeyspaceInfo {

    private String name;
    private Boolean durableWrites;
    private String replicationFormatted;
}
