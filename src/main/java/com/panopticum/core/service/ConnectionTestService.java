package com.panopticum.core.service;

import com.panopticum.cassandra.service.CassandraMetadataService;
import com.panopticum.clickhouse.service.ClickHouseMetadataService;
import com.panopticum.mongo.service.MongoMetadataService;
import com.panopticum.mssql.service.MssqlMetadataService;
import com.panopticum.oracle.service.OracleMetadataService;
import com.panopticum.mysql.service.MySqlMetadataService;
import com.panopticum.postgres.service.PgMetadataService;
import com.panopticum.elasticsearch.service.ElasticsearchService;
import com.panopticum.kafka.service.KafkaService;
import com.panopticum.rabbitmq.service.RabbitMqService;
import com.panopticum.redis.service.RedisMetadataService;

import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class ConnectionTestService {

    private final PgMetadataService pgMetadataService;
    private final MongoMetadataService mongoMetadataService;
    private final RedisMetadataService redisMetadataService;
    private final ClickHouseMetadataService clickHouseMetadataService;
    private final MySqlMetadataService mySqlMetadataService;
    private final MssqlMetadataService mssqlMetadataService;
    private final OracleMetadataService oracleMetadataService;
    private final CassandraMetadataService cassandraMetadataService;
    private final RabbitMqService rabbitMqService;
    private final KafkaService kafkaService;
    private final ElasticsearchService elasticsearchService;

    public Optional<String> test(String type, String host, Integer port, String database,
                                String username, String password) {
        if (type == null || type.isBlank()) {
            return Optional.of("error.specifyHostDbUser");
        }
        int p = port != null ? port : defaultPort(type);
        String h = host != null ? host : "";
        String db = database != null ? database : "";
        String user = username != null ? username : "";
        String pass = password != null ? password : "";

        return switch (type.toLowerCase()) {
            case "postgresql" -> pgMetadataService.testConnection(h, p, db, user, pass);
            case "mongodb" -> mongoMetadataService.testConnection(h, p, db, user, pass);
            case "redis" -> {
                int dbIndex = 0;
                if (!db.isBlank()) {
                    try {
                        dbIndex = Integer.parseInt(db.trim());
                    } catch (NumberFormatException ignored) {
                    }
                }
                yield redisMetadataService.testConnection(h, p, pass, dbIndex);
            }
            case "clickhouse" -> clickHouseMetadataService.testConnection(h, p,
                    db.isBlank() ? "default" : db, user, pass);
            case "mysql" -> mySqlMetadataService.testConnection(h, p, db, user, pass);
            case "sqlserver" -> mssqlMetadataService.testConnection(h, p, db, user, pass);
            case "oracle" -> oracleMetadataService.testConnection(h, p, db, user, pass);
            case "cassandra" -> cassandraMetadataService.testConnection(h, p, db, user, pass);
            case "rabbitmq" -> rabbitMqService.testConnection(h, p, db, user, pass);
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
            case "elasticsearch" -> elasticsearchService.testConnection(h, p, user, pass);
            default -> Optional.of("error.specifyHostDbUser");
        };
    }

    private static int defaultPort(String type) {
        if (type == null) {
            return 5432;
        }
        return switch (type.toLowerCase()) {
            case "postgresql" -> 5432;
            case "mongodb" -> 27017;
            case "redis" -> 6379;
            case "clickhouse" -> 8123;
            case "mysql" -> 3306;
            case "sqlserver" -> 1433;
            case "oracle" -> 1521;
            case "cassandra" -> 9042;
            case "rabbitmq" -> 15672;
            case "kafka" -> 9092;
            case "elasticsearch" -> 9200;
            default -> 5432;
        };
    }
}
