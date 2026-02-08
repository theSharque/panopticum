package com.panopticum.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DbConnection {

    private Long id;
    private String name;
    private String type;
    private String host;
    private int port;
    private String dbName;
    private String username;
    private String password;
    private String createdAt;
}
