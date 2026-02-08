package com.panopticum.redis.service;

import com.panopticum.core.util.StringUtils;
import com.panopticum.redis.model.RedisDbInfo;
import com.panopticum.redis.model.RedisKeyDetail;
import com.panopticum.redis.model.RedisKeyInfo;
import com.panopticum.redis.model.RedisKeysPage;
import com.panopticum.redis.repository.RedisMetadataRepository;
import io.lettuce.core.RedisURI;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Singleton
@Slf4j
public class RedisMetadataService {

    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_DATABASES = 16;

    private final RedisMetadataRepository redisMetadataRepository;

    @Value("${panopticum.limits.redis.keys-per-page:100}")
    private int keysPerPage;

    @Value("${panopticum.limits.redis.value-preview-length:10000}")
    private int valuePreviewLength;

    public RedisMetadataService(RedisMetadataRepository redisMetadataRepository) {
        this.redisMetadataRepository = redisMetadataRepository;
    }

    public Optional<String> testConnection(String host, int port, String password, int dbIndex) {
        if (host == null || host.isBlank()) {
            return Optional.of("error.specifyHost");
        }
        RedisURI uri = redisMetadataRepository.buildUri(host.trim(), port > 0 ? port : DEFAULT_PORT, null, password, dbIndex);
        try (var client = io.lettuce.core.RedisClient.create(uri);
             var connection = client.connect()) {
            connection.sync().ping();
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public List<RedisDbInfo> listDatabases(Long connectionId) {
        return listDatabasesSorted(connectionId, "dbIndex", "asc");
    }

    public List<RedisDbInfo> listDatabasesSorted(Long connectionId, String sort, String order) {
        List<RedisDbInfo> result = new ArrayList<>();
        for (int i = 0; i < DEFAULT_DATABASES; i++) {
            int dbIndex = i;
            redisMetadataRepository.withConnection(connectionId, dbIndex, (cmd, dbCmd) -> {
                long size = dbCmd.dbsize();
                return new RedisDbInfo(dbIndex, size);
            }).ifPresent(result::add);
        }
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "dbIndex";
        Comparator<RedisDbInfo> comparator = "keyCount".equals(sortBy)
                ? (desc ? Comparator.comparingLong(RedisDbInfo::getKeyCount).reversed() : Comparator.comparingLong(RedisDbInfo::getKeyCount))
                : (desc ? Comparator.comparingInt(RedisDbInfo::getDbIndex).reversed() : Comparator.comparingInt(RedisDbInfo::getDbIndex));
        return result.stream().sorted(comparator).toList();
    }

    public List<RedisKeyInfo> sortKeys(List<RedisKeyInfo> keys, String sort, String order) {
        if (keys == null || keys.isEmpty()) {
            return keys != null ? keys : List.of();
        }
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "key";
        java.util.Comparator<RedisKeyInfo> comparator = switch (sortBy) {
            case "type" -> desc
                    ? Comparator.comparing(RedisKeyInfo::getType, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)).reversed()
                    : Comparator.comparing(RedisKeyInfo::getType, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "ttl" -> desc
                    ? Comparator.comparing(RedisKeyInfo::getTtl, Comparator.nullsFirst(Long::compareTo)).reversed()
                    : Comparator.comparing(RedisKeyInfo::getTtl, Comparator.nullsLast(Long::compareTo));
            default -> desc
                    ? Comparator.comparing(RedisKeyInfo::getKey, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)).reversed()
                    : Comparator.comparing(RedisKeyInfo::getKey, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
        };
        return keys.stream().sorted(comparator).toList();
    }

    public RedisKeysPage listKeys(Long connectionId, int dbIndex, String pattern, String cursor, int limit) {
        String pat = pattern != null && !pattern.isBlank() ? pattern : "*";
        int lim = limit > 0 ? Math.min(limit, keysPerPage) : keysPerPage;
        return redisMetadataRepository.withConnection(connectionId, dbIndex, (cmd, dbCmd) -> {
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
        return redisMetadataRepository.withConnection(connectionId, dbIndex, (cmd, dbCmd) -> {
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

    public Optional<String> setKey(Long connectionId, int dbIndex, String key, String value) {
        if (key == null || key.isBlank()) {
            return Optional.of("Key is required.");
        }
        Optional<Object> ok = redisMetadataRepository.withConnection(connectionId, dbIndex, (cmd, dbCmd) -> {
            cmd.set(key, value != null ? value : "");
            return null;
        });
        return ok.isEmpty() ? Optional.of("error.connectionNotAvailable") : Optional.empty();
    }

    public Optional<String> setHash(Long connectionId, int dbIndex, String key, Map<String, String> fields) {
        if (key == null || key.isBlank()) {
            return Optional.of("Key is required.");
        }
        Optional<Object> ok = redisMetadataRepository.withConnection(connectionId, dbIndex, (cmd, dbCmd) -> {
            cmd.del(key);
            if (fields != null && !fields.isEmpty()) {
                cmd.hset(key, fields);
            }
            return null;
        });
        return ok.isEmpty() ? Optional.of("error.connectionNotAvailable") : Optional.empty();
    }
}
