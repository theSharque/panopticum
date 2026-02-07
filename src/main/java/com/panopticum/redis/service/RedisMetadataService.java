package com.panopticum.redis.service;

import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.StringUtils;
import com.panopticum.redis.model.RedisDbInfo;
import com.panopticum.redis.model.RedisKeyDetail;
import com.panopticum.redis.model.RedisKeyInfo;
import com.panopticum.redis.model.RedisKeysPage;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Singleton
@Slf4j
public class RedisMetadataService {

    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_DATABASES = 16;

    private final DbConnectionService dbConnectionService;

    @Value("${panopticum.limits.redis.keys-per-page:100}")
    private int keysPerPage;

    @Value("${panopticum.limits.redis.value-preview-length:10000}")
    private int valuePreviewLength;

    public RedisMetadataService(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    public Optional<String> testConnection(String host, int port, String password, int dbIndex) {
        if (host == null || host.isBlank()) {
            return Optional.of("error.specifyHost");
        }

        RedisURI uri = buildUri(host.trim(), port > 0 ? port : DEFAULT_PORT, null, password, dbIndex);

        try (var client = io.lettuce.core.RedisClient.create(uri);
             StatefulRedisConnection<String, String> connection = client.connect()) {
            connection.sync().ping();

            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public List<RedisDbInfo> listDatabases(Long connectionId) {
        List<RedisDbInfo> result = new ArrayList<>();
        for (int i = 0; i < DEFAULT_DATABASES; i++) {
            int dbIndex = i;
            withConnection(connectionId, dbIndex, (cmd, dbCmd) -> {
                long size = dbCmd.dbsize();
                return new RedisDbInfo(dbIndex, size);
            }).ifPresent(result::add);
        }

        return result;
    }

    public RedisKeysPage listKeys(Long connectionId, int dbIndex, String pattern, String cursor, int limit) {
        String pat = pattern != null && !pattern.isBlank() ? pattern : "*";
        int lim = limit > 0 ? Math.min(limit, keysPerPage) : keysPerPage;

        return withConnection(connectionId, dbIndex, (cmd, dbCmd) -> {
            io.lettuce.core.ScanCursor sc = "0".equals(cursor) || cursor == null || cursor.isBlank()
                    ? io.lettuce.core.ScanCursor.INITIAL
                    : io.lettuce.core.ScanCursor.of(cursor);

            io.lettuce.core.ScanArgs args = io.lettuce.core.ScanArgs.Builder.matches(pat).limit(lim);
            io.lettuce.core.KeyScanCursor<String> scanResult = cmd.scan(sc, args);

            List<RedisKeyInfo> infos = new ArrayList<>();
            for (String key : scanResult.getKeys()) {
                try {
                    String type = cmd.type(key);
                    Long ttl = cmd.pttl(key);
                    if (ttl != null && ttl < 0) {
                        ttl = null;
                    }
                    infos.add(new RedisKeyInfo(key, type != null ? type : "unknown", ttl));
                } catch (Exception e) {
                    log.debug("TYPE/TTL for key {} failed: {}", key, e.getMessage());
                    infos.add(new RedisKeyInfo(key, "?", null));
                }
            }

            return new RedisKeysPage(infos,
                    scanResult.getCursor() != null ? scanResult.getCursor() : "0",
                    !scanResult.isFinished());
        }).orElse(RedisKeysPage.empty());
    }

    public Optional<RedisKeyDetail> getKeyDetail(Long connectionId, int dbIndex, String key) {
        if (key == null || key.isBlank()) {
            return Optional.empty();
        }

        return withConnectionForKeyDetail(connectionId, dbIndex, (cmd, dbCmd) -> {
            String type = cmd.type(key);
            if (type == null || "none".equalsIgnoreCase(type)) {
                return null;
            }

            Long ttl = cmd.pttl(key);
            if (ttl != null && ttl < 0) {
                ttl = null;
            }

            Object value = switch (type.toLowerCase()) {
                case "string" -> StringUtils.truncate(cmd.get(key));
                case "hash" -> cmd.hgetall(key);
                case "list" -> cmd.lrange(key, 0, valuePreviewLength - 1);
                case "set" -> cmd.smembers(key);
                case "zset" -> cmd.zrangeWithScores(key, 0, valuePreviewLength - 1);
                default -> null;
            };

            return new RedisKeyDetail(key, type, ttl, value);
        });
    }

    private Optional<RedisKeyDetail> withConnectionForKeyDetail(Long connectionId, int dbIndex, ConnectionCallback<RedisKeyDetail> callback) {
        return withConnection(connectionId, dbIndex, callback);
    }

    private <T> Optional<T> withConnection(Long connectionId, int dbIndex, ConnectionCallback<T> callback) {
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

    private RedisURI buildUri(String host, int port, String username, String password, int database) {
        RedisURI.Builder builder = RedisURI.builder()
                .withHost(host)
                .withPort(port > 0 ? port : DEFAULT_PORT)
                .withDatabase(database >= 0 ? database : 0);

        if (password != null && !password.isBlank()) {
            builder.withPassword(password.toCharArray());
        }

        return builder.build();
    }

    private interface ConnectionCallback<T> {
        T apply(RedisCommands<String, String> cmd, RedisServerCommands<String, String> dbCmd);
    }
}
