package com.panopticum.cassandra.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CassandraTableInfo {

    private String name;
    private String type;
    private String comment;
    private Integer defaultTimeToLive;
    private Integer gcGraceSeconds;
}
