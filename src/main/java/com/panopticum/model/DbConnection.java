package com.panopticum.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
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
