package com.panopticum.core.model;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public enum ConnectionType {

    POSTGRESQL("postgresql", Set.of(), 5432, "/postgres", "/api/postgres/connections",
            "catalog.namespace.entity", "sql", false, ""),
    GREENPLUM("greenplum", Set.of(), 5432, "/postgres", "/api/postgres/connections",
            "catalog.namespace.entity", "sql", false, ""),
    YUGABYTEDB("yugabytedb", Set.of(), 5433, "/postgres", "/api/postgres/connections",
            "catalog.namespace.entity", "sql", false, ""),
    COCKROACHDB("cockroachdb", Set.of(), 26257, "/postgres", "/api/postgres/connections",
            "catalog.namespace.entity", "sql", false, ""),
    H2("h2", Set.of(), 9092, "/lightjdbc", "/api/lightjdbc/connections",
            "catalog.namespace.entity", "sql", false, ""),
    HSQLDB("hsqldb", Set.of(), 9001, "/lightjdbc", "/api/lightjdbc/connections",
            "catalog.namespace.entity", "sql", false, ""),
    DERBY("derby", Set.of(), 1527, "/lightjdbc", "/api/lightjdbc/connections",
            "catalog.namespace.entity", "sql", false, ""),
    MYSQL("mysql", Set.of(), 3306, "/mysql", "/api/mysql/connections",
            "catalog.entity", "sql", false, ""),
    SQLSERVER("sqlserver", Set.of("mssql"), 1433, "/sqlserver", "/api/sqlserver/connections",
            "catalog.namespace.entity", "sql", false, "master"),
    ORACLE("oracle", Set.of(), 1521, "/oracle", "/api/oracle/connections",
            "namespace.entity", "sql", false, "XEPDB1"),
    MONGODB("mongodb", Set.of(), 27017, "/mongo", "/api/mongo/connections",
            "catalog.entity", "mql-json", false, ""),
    REDIS("redis", Set.of(), 6379, "/redis", "/api/redis/connections",
            "catalog", "pattern", false, "0"),
    CLICKHOUSE("clickhouse", Set.of(), 8123, "/clickhouse", "/api/clickhouse/connections",
            "catalog.entity", "sql", false, "default"),
    CASSANDRA("cassandra", Set.of(), 9042, "/cassandra", "/api/cassandra/connections",
            "catalog.entity", "cql", false, ""),
    COUCHBASE("couchbase", Set.of(), 11210, "/couchbase", "/api/couchbase/connections",
            "catalog.namespace.entity", "n1ql", true, ""),
    KAFKA("kafka", Set.of(), 9092, "/kafka", "/api/kafka/connections",
            "catalog.entity", "json", false, ""),
    RABBITMQ("rabbitmq", Set.of(), 15672, "/rabbitmq", "/api/rabbitmq/connections",
            "catalog.entity", "message-peek", false, "/"),
    ELASTICSEARCH("elasticsearch", Set.of(), 9200, "/elasticsearch", "/api/elasticsearch/connections",
            "catalog", "json", true, ""),
    KUBERNETES("kubernetes", Set.of(), 443, "/kubernetes", null,
            "catalog.entity", "tail", false, ""),
    S3("s3", Set.of(), 443, "/s3", null,
            "catalog.entity", "object-peek", true, ""),
    PROMETHEUS("prometheus", Set.of(), 9090, "/prometheus", null,
            "catalog.entity", "promql", true, "");

    private final String canonicalId;
    private final Set<String> aliases;
    private final int defaultPort;
    private final String uiPathPrefix;
    private final String apiPathPrefix;
    private final String hierarchyModel;
    private final String queryFormat;
    private final boolean supportsHttps;
    private final String defaultDatabase;

    ConnectionType(String canonicalId, Set<String> aliases, int defaultPort, String uiPathPrefix,
                   String apiPathPrefix, String hierarchyModel, String queryFormat,
                   boolean supportsHttps, String defaultDatabase) {
        this.canonicalId = canonicalId;
        this.aliases = aliases;
        this.defaultPort = defaultPort;
        this.uiPathPrefix = uiPathPrefix;
        this.apiPathPrefix = apiPathPrefix;
        this.hierarchyModel = hierarchyModel;
        this.queryFormat = queryFormat;
        this.supportsHttps = supportsHttps;
        this.defaultDatabase = defaultDatabase;
    }

    public String getCanonicalId() {
        return canonicalId;
    }

    public int getDefaultPort() {
        return defaultPort;
    }

    public String getUiPathPrefix() {
        return uiPathPrefix;
    }

    public String getApiPathPrefix() {
        return apiPathPrefix;
    }

    public String getHierarchyModel() {
        return hierarchyModel;
    }

    public String getQueryFormat() {
        return queryFormat;
    }

    public boolean supportsHttps() {
        return supportsHttps;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public static Optional<ConnectionType> fromStoredType(String type) {
        if (type == null || type.isBlank()) {
            return Optional.empty();
        }
        String normalized = type.toLowerCase(Locale.ROOT);
        return Arrays.stream(values())
                .filter(ct -> ct.canonicalId.equals(normalized) || ct.aliases.contains(normalized))
                .findFirst();
    }

    public static String normalizeTypeId(String type) {
        return fromStoredType(type).map(ConnectionType::getCanonicalId).orElse("");
    }

    public static int defaultPortFor(String type) {
        return fromStoredType(type).map(ConnectionType::getDefaultPort).orElse(POSTGRESQL.defaultPort);
    }

    public static String defaultDatabaseFor(String type) {
        return fromStoredType(type).map(ConnectionType::getDefaultDatabase).orElse("");
    }

    public static String queryFormatFor(String type) {
        return fromStoredType(type).map(ConnectionType::getQueryFormat).orElse("sql");
    }

    public static String hierarchyModelFor(String type) {
        return fromStoredType(type).map(ConnectionType::getHierarchyModel).orElse("catalog.entity");
    }

    public static boolean isSqlType(String typeId) {
        if (typeId == null || typeId.isBlank()) {
            return false;
        }
        return switch (typeId) {
            case "postgresql", "greenplum", "yugabytedb", "cockroachdb",
                    "mysql", "sqlserver", "oracle", "clickhouse",
                    "h2", "hsqldb", "derby", "cassandra" -> true;
            default -> false;
        };
    }
}
