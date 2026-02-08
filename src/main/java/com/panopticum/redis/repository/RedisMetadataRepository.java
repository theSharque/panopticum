package com.panopticum.redis.repository;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.redis.model.RedisConnectionCallback;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Singleton
@Slf4j
public class RedisMetadataRepository {

    private static final int DEFAULT_PORT = 6379;

    private final DbConnectionService dbConnectionService;

    public RedisMetadataRepository(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    public RedisURI buildUri(String host, int port, String username, String password, int database) {
        RedisURI.Builder builder = RedisURI.builder()
                .withHost(host)
                .withPort(port > 0 ? port : DEFAULT_PORT)
                .withDatabase(database >= 0 ? database : 0);
        if (password != null && !password.isBlank()) {
            builder.withPassword(password.toCharArray());
        }
        return builder.build();
    }

    public <T> Optional<T> withConnection(Long connectionId, int dbIndex, RedisConnectionCallback<T> callback) {
        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId)
                .filter(c -> "redis".equalsIgnoreCase(c.getType()));
        if (connOpt.isEmpty()) {
            return Optional.empty();
        }
        DbConnection conn = connOpt.get();
        RedisURI uri = buildUri(conn.getHost(), conn.getPort(), conn.getUsername(), conn.getPassword(), dbIndex);
        try (var client = io.lettuce.core.RedisClient.create(uri);
             StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> cmd = connection.sync();
            RedisServerCommands<String, String> dbCmd = connection.sync();
            return Optional.of(callback.apply(cmd, dbCmd));
        } catch (Exception e) {
            log.warn("Redis operation failed for {}: {}", conn.getName(), e.getMessage());
            return Optional.empty();
        }
    }
}
