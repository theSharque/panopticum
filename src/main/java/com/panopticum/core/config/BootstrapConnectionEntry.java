package com.panopticum.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class BootstrapConnectionEntry {

    private String name;
    private String type;
    private String host;
    private Integer port;
    private String database;
    private String username;
    private String password;

    @JsonProperty("jdbcUrl")
    private String jdbcUrl;

    @JsonProperty("url")
    public void setUrl(String url) {
        if (this.jdbcUrl == null) {
            this.jdbcUrl = url;
        }
    }
}
