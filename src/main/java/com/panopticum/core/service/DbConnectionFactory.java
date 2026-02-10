package com.panopticum.core.service;

import com.panopticum.core.model.DbConnection;

import jakarta.inject.Singleton;

@Singleton
public class DbConnectionFactory {

    private static final int PORT_POSTGRESQL = 5432;
    private static final int PORT_MONGODB = 27017;
    private static final int PORT_REDIS = 6379;
    private static final int PORT_CLICKHOUSE = 8123;
    private static final int PORT_MYSQL = 3306;
    private static final int PORT_CASSANDRA = 9042;
    private static final int PORT_MSSQL = 1433;

    public DbConnection build(String type, String name, String host, Integer port,
                             String database, String username, String password) {
        if (type == null || type.isBlank()) {
            return DbConnection.builder()
                    .name(nullToEmpty(name))
                    .type("")
                    .host(nullToEmpty(host))
                    .port(5432)
                    .dbName(nullToEmpty(database))
                    .username(nullToEmpty(username))
                    .password(nullToEmpty(password))
                    .build();
        }
        int defaultPort = defaultPortForType(type);
        int p = port != null ? port : defaultPort;
        String db = database != null ? database : defaultDatabaseForType(type);
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
                .build();
    }

    private static String nullToEmpty(String s) {
        return s != null ? s : "";
    }

    private static int defaultPortForType(String type) {
        if (type == null) {
            return PORT_POSTGRESQL;
        }
        return switch (type.toLowerCase()) {
            case "postgresql" -> PORT_POSTGRESQL;
            case "mongodb" -> PORT_MONGODB;
            case "redis" -> PORT_REDIS;
            case "clickhouse" -> PORT_CLICKHOUSE;
            case "mysql" -> PORT_MYSQL;
            case "cassandra" -> PORT_CASSANDRA;
            case "sqlserver" -> PORT_MSSQL;
            default -> PORT_POSTGRESQL;
        };
    }

    private static String defaultDatabaseForType(String type) {
        if (type == null) {
            return "";
        }
        return switch (type.toLowerCase()) {
            case "redis" -> "0";
            case "clickhouse" -> "default";
            case "sqlserver" -> "master";
            default -> "";
        };
    }
}
