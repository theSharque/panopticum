package com.panopticum.core.service;

import com.panopticum.cassandra.service.CassandraMetadataService;
import com.panopticum.clickhouse.service.ClickHouseMetadataService;
import com.panopticum.couchbase.service.CouchbaseMetadataService;
import com.panopticum.lightjdbc.service.LightJdbcMetadataService;
import com.panopticum.mongo.service.MongoMetadataService;
import com.panopticum.sqlserver.service.SqlServerMetadataService;
import com.panopticum.oracle.service.OracleMetadataService;
import com.panopticum.mysql.service.MySqlMetadataService;
import com.panopticum.postgres.service.PostgresMetadataService;
import com.panopticum.elasticsearch.service.ElasticsearchMetadataService;
import com.panopticum.kafka.service.KafkaService;
import com.panopticum.kubernetes.service.KubernetesService;
import com.panopticum.prometheus.service.PrometheusService;
import com.panopticum.rabbitmq.service.RabbitMqMetadataService;
import com.panopticum.redis.service.RedisMetadataService;
import com.panopticum.s3.service.S3Service;
import com.panopticum.core.model.ConnectionType;
import com.panopticum.core.model.DbConnection;

import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class ConnectionTestService {

    private final PostgresMetadataService postgresMetadataService;
    private final MongoMetadataService mongoMetadataService;
    private final RedisMetadataService redisMetadataService;
    private final ClickHouseMetadataService clickHouseMetadataService;
    private final MySqlMetadataService mySqlMetadataService;
    private final SqlServerMetadataService sqlServerMetadataService;
    private final OracleMetadataService oracleMetadataService;
    private final CassandraMetadataService cassandraMetadataService;
    private final RabbitMqMetadataService rabbitMqMetadataService;
    private final KafkaService kafkaService;
    private final ElasticsearchMetadataService elasticsearchMetadataService;
    private final KubernetesService kubernetesService;
    private final S3Service s3Service;
    private final PrometheusService prometheusService;
    private final LightJdbcMetadataService lightJdbcMetadataService;
    private final CouchbaseMetadataService couchbaseMetadataService;
    private final DbConnectionService dbConnectionService;

    public Optional<String> test(String type, String host, Integer port, String database,
                                String username, String password, Optional<Long> connectionId) {
        return test(type, host, port, database, username, password, connectionId, null, null);
    }

    public Optional<String> test(String type, String host, Integer port, String database,
                                String username, String password, Optional<Long> connectionId, Boolean couchbaseTlsOverride) {
        return test(type, host, port, database, username, password, connectionId, couchbaseTlsOverride, null);
    }

    public Optional<String> test(String type, String host, Integer port, String database,
                                String username, String password, Optional<Long> connectionId,
                                Boolean couchbaseTlsOverride, Boolean useHttpsOverride) {
        if (type == null || type.isBlank()) {
            return Optional.of("error.specifyHostDbUser");
        }
        int p = port != null ? port : ConnectionType.defaultPortFor(type);
        String h = host != null ? host : "";
        String db = database != null ? database : "";
        String user = username != null ? username : "";
        String pass = password != null ? password : "";

        return switch (type.toLowerCase()) {
            case "postgresql", "greenplum", "yugabytedb", "cockroachdb" -> postgresMetadataService.testConnection(h, p, db, user, pass);
            case "mongodb" -> mongoMetadataService.testConnection(h, p, db, user, pass);
            case "redis" -> {
                int dbIndex = 0;
                if (!db.isBlank()) {
                    try {
                        dbIndex = Integer.parseInt(db.trim());
                    } catch (NumberFormatException e) {
                        log.warn("Invalid Redis DB index '{}', using default 0", db.trim());
                    }
                }
                yield redisMetadataService.testConnection(h, p, pass, dbIndex);
            }
            case "clickhouse" -> clickHouseMetadataService.testConnection(h, p,
                    db.isBlank() ? "default" : db, user, pass);
            case "mysql" -> mySqlMetadataService.testConnection(h, p, db, user, pass);
            case "sqlserver" -> sqlServerMetadataService.testConnection(h, p, db, user, pass);
            case "oracle" -> oracleMetadataService.testConnection(h, p, db, user, pass);
            case "cassandra" -> cassandraMetadataService.testConnection(h, p, db, user, pass);
            case "rabbitmq" -> rabbitMqMetadataService.testConnection(h, p, db, user, pass);
            case "kafka" -> {
                log.info("Testing Kafka connection: host={}, port={}", h, p);
                Optional<String> err = kafkaService.testConnection(h, p, db, user, pass);
                if (err.isPresent()) {
                    log.warn("Kafka connection test failed: host={}, port={}, errorKey={}", h, p, err.get());
                } else {
                    log.info("Kafka connection test OK: host={}, port={}", h, p);
                }
                yield err;
            }
            case "elasticsearch" -> elasticsearchMetadataService.testConnection(connectionId, h, p, user, pass);
            case "kubernetes" -> kubernetesService.testConnection(h, p, db, pass);
            case "s3" -> s3Service.testConnection(h, p, db, user, pass, resolveHttps(connectionId, useHttpsOverride));
            case "prometheus" -> prometheusService.testConnection(h, p, user, pass, resolveHttps(connectionId, useHttpsOverride));
            case "h2", "hsqldb", "derby" -> lightJdbcMetadataService.testConnection(h, p, db, user, pass, type.toLowerCase());
            case "couchbase" -> couchbaseMetadataService.testProbe(h, p, db, user, pass, couchbaseTls(connectionId, couchbaseTlsOverride));
            default -> Optional.of("error.specifyHostDbUser");
        };
    }

    private boolean couchbaseTls(Optional<Long> connectionId, Boolean override) {
        if (override != null) {
            return override;
        }
        return connectionId.flatMap(dbConnectionService::findById).map(DbConnection::isUseHttps).orElse(false);
    }

    private boolean resolveHttps(Optional<Long> connectionId, Boolean override) {
        if (override != null) {
            return override;
        }
        return connectionId.flatMap(dbConnectionService::findById).map(DbConnection::isUseHttps).orElse(false);
    }
}
