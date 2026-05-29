package com.panopticum.core.service;

import com.panopticum.core.model.ConnectionType;
import com.panopticum.core.model.DbConnection;

import jakarta.inject.Singleton;

@Singleton
public class DbConnectionFactory {

    public DbConnection build(String type, String name, String host, Integer port,
                             String database, String username, String password) {
        if (type == null || type.isBlank()) {
            return DbConnection.builder()
                    .name(nullToEmpty(name))
                    .type("")
                    .host(nullToEmpty(host))
                    .port(ConnectionType.POSTGRESQL.getDefaultPort())
                    .dbName(nullToEmpty(database))
                    .username(nullToEmpty(username))
                    .password(nullToEmpty(password))
                    .useHttps(false)
                    .build();
        }
        int defaultPort = ConnectionType.defaultPortFor(type);
        int p = (port != null && port > 0) ? port : defaultPort;
        String db = database != null ? database : ConnectionType.defaultDatabaseFor(type);
        String user = username != null ? username : "";
        if ("redis".equalsIgnoreCase(type)) {
            user = "";
        }
        return DbConnection.builder()
                .name(nullToEmpty(name))
                .type(type)
                .host(host != null && !host.isBlank() ? host : "localhost")
                .port(p)
                .dbName(db)
                .username(user)
                .password(password != null ? password : "")
                .useHttps(false)
                .build();
    }

    private static String nullToEmpty(String s) {
        return s != null ? s : "";
    }
}
