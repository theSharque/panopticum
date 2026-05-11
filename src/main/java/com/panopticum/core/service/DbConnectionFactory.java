package com.panopticum.core.service;

import com.panopticum.core.model.DbConnection;

import jakarta.inject.Singleton;

@Singleton
public class DbConnectionFactory {

    private static final int PORT_POSTGRESQL = 5432;
    private static final int PORT_YUGABYTEDB = 5433;
    private static final int PORT_COCKROACHDB = 26257;
    private static final int PORT_H2 = 9092;
    private static final int PORT_HSQLDB = 9001;
    private static final int PORT_DERBY = 1527;
    private static final int PORT_COUCHBASE = 11210;
    private static final int PORT_MONGODB = 27017;
    private static final int PORT_REDIS = 6379;
    private static final int PORT_CLICKHOUSE = 8123;
    private static final int PORT_MYSQL = 3306;
    private static final int PORT_CASSANDRA = 9042;
    private static final int PORT_MSSQL = 1433;
    private static final int PORT_ORACLE = 1521;
    private static final int PORT_RABBITMQ = 15672;
    private static final int PORT_KAFKA = 9092;
    private static final int PORT_ELASTICSEARCH = 9200;
    private static final int PORT_KUBERNETES = 443;
    private static final int PORT_S3 = 443;
    private static final int PORT_PROMETHEUS = 9090;

    public DbConnection build(String type, String name, String host, Integer port,
                             String database, String username, String password) {
        if (type == null || type.isBlank()) {
            return DbConnection.builder()
                    .name(nullToEmpty(name))
                    .type("")
                    .host(nullToEmpty(host))
                    .port(PORT_POSTGRESQL)
                    .dbName(nullToEmpty(database))
                    .username(nullToEmpty(username))
                    .password(nullToEmpty(password))
                    .useHttps(false)
                    .build();
        }
        int defaultPort = defaultPortForType(type);
        int p = (port != null && port > 0) ? port : defaultPort;
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
                .useHttps(false)
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
            case "greenplum" -> PORT_POSTGRESQL;
            case "yugabytedb" -> PORT_YUGABYTEDB;
            case "cockroachdb" -> PORT_COCKROACHDB;
            case "h2" -> PORT_H2;
            case "hsqldb" -> PORT_HSQLDB;
            case "derby" -> PORT_DERBY;
            case "redis" -> PORT_REDIS;
            case "mongodb" -> PORT_MONGODB;
            case "clickhouse" -> PORT_CLICKHOUSE;
            case "mysql" -> PORT_MYSQL;
            case "cassandra" -> PORT_CASSANDRA;
            case "sqlserver" -> PORT_MSSQL;
            case "oracle" -> PORT_ORACLE;
            case "rabbitmq" -> PORT_RABBITMQ;
            case "kafka" -> PORT_KAFKA;
            case "elasticsearch" -> PORT_ELASTICSEARCH;
            case "kubernetes" -> PORT_KUBERNETES;
            case "s3" -> PORT_S3;
            case "prometheus" -> PORT_PROMETHEUS;
            case "couchbase" -> PORT_COUCHBASE;
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
            case "oracle" -> "XEPDB1";
            case "rabbitmq" -> "/";
            case "kafka" -> "";
            case "kubernetes" -> "";
            case "s3" -> "";
            case "prometheus" -> "";
            case "couchbase" -> "";
            default -> "";
        };
    }
}
