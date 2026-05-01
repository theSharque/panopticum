package com.panopticum.s3.service;

import com.panopticum.core.error.AccessResult;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.SizeFormatter;
import com.panopticum.mcp.model.ColumnInfo;
import com.panopticum.mcp.model.EntityDescription;
import com.panopticum.s3.model.S3BucketInfo;
import com.panopticum.s3.model.S3ObjectInfo;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListBucketsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class S3Service {

    private static final int DEFAULT_PEEK_BYTES = 65536;
    private static final int MAX_PEEK_BYTES = 1048576;

    private final DbConnectionService dbConnectionService;

    public Optional<String> testConnection(String host, int port, String region, String accessKey, String secretKey, boolean useHttps) {
        try {
            MinioClient client = buildClient(host, port, accessKey, secretKey, useHttps);
            client.listBuckets(ListBucketsArgs.builder().build());
            return Optional.empty();
        } catch (ErrorResponseException e) {
            String code = e.errorResponse().code();
            if ("AccessDenied".equals(code) || "SignatureDoesNotMatch".equals(code)) {
                return Optional.of("s3.access.forbidden");
            }
            return Optional.of("s3.access.error");
        } catch (Exception e) {
            log.warn("S3 test connection failed: {}", e.getMessage());
            return Optional.of("s3.access.error");
        }
    }

    public AccessResult<List<S3BucketInfo>> listBuckets(Long connectionId) {
        return withClient(connectionId, client -> {
            List<Bucket> buckets = client.listBuckets(ListBucketsArgs.builder().build());
            List<S3BucketInfo> result = new ArrayList<>();
            for (Bucket b : buckets) {
                result.add(S3BucketInfo.builder()
                        .name(b.name())
                        .createdAt(b.creationDate() != null ? b.creationDate().toString() : "")
                        .build());
            }
            return AccessResult.ok(result);
        });
    }

    public AccessResult<Page<S3ObjectInfo>> listObjects(Long connectionId, String bucket, String prefix, int page, int size) {
        return withClient(connectionId, client -> {
            String safePrefix = prefix != null ? prefix : "";
            ListObjectsArgs args = ListObjectsArgs.builder()
                    .bucket(bucket)
                    .prefix(safePrefix)
                    .delimiter("/")
                    .build();
            List<S3ObjectInfo> items = new ArrayList<>();
            for (Result<Item> result : client.listObjects(args)) {
                Item item = result.get();
                items.add(S3ObjectInfo.builder()
                        .key(item.objectName())
                        .size(SizeFormatter.formatSize(item.size()))
                        .sizeBytes(item.size())
                        .lastModified(item.lastModified() != null ? item.lastModified().toString() : "")
                        .etag(item.etag() != null ? item.etag() : "")
                        .prefix(item.isDir())
                        .build());
            }
            int offset = (page - 1) * size;
            int end = Math.min(offset + size, items.size());
            List<S3ObjectInfo> slice = offset < items.size() ? items.subList(offset, end) : List.of();
            return AccessResult.ok(Page.of(slice, page, size, "key", "asc"));
        });
    }

    public AccessResult<String> peekObject(Long connectionId, String bucket, String key, int headBytes, String format) {
        return withClient(connectionId, client -> {
            int safeBytes = Math.min(Math.max(1, headBytes), MAX_PEEK_BYTES);
            try (InputStream in = client.getObject(GetObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .offset(0L)
                    .length((long) safeBytes)
                    .build())) {
                byte[] buf = in.readNBytes(safeBytes);
                String resolvedFormat = resolveFormat(format, key);
                return AccessResult.ok(formatContent(buf, resolvedFormat));
            }
        });
    }

    public AccessResult<EntityDescription> describeObject(Long connectionId, String bucket, String key) {
        return withClient(connectionId, client -> {
            StatObjectResponse stat = client.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build());
            List<ColumnInfo> columns = new ArrayList<>();
            columns.add(ColumnInfo.builder().name("key").type("string").nullable(false).primaryKey(true).position(1).build());
            columns.add(ColumnInfo.builder().name("size").type("int64").nullable(false).primaryKey(false).position(2).build());
            columns.add(ColumnInfo.builder().name("etag").type("string").nullable(true).primaryKey(false).position(3).build());
            columns.add(ColumnInfo.builder().name("content_type").type("string").nullable(true).primaryKey(false).position(4).build());
            if (stat.userMetadata() != null) {
                int pos = 5;
                for (String metaKey : stat.userMetadata().keySet()) {
                    columns.add(ColumnInfo.builder().name("x-amz-meta-" + metaKey).type("string").nullable(true).primaryKey(false).position(pos++).build());
                }
            }
            List<String> notes = new ArrayList<>();
            notes.add("size=" + stat.size());
            notes.add("contentType=" + stat.contentType());
            notes.add("etag=" + stat.etag());

            return AccessResult.ok(EntityDescription.builder()
                    .entityKind("object")
                    .catalog(bucket)
                    .namespace(null)
                    .entity(key)
                    .columns(columns)
                    .primaryKey(List.of("key"))
                    .foreignKeys(List.of())
                    .indexes(List.of())
                    .approximateRowCount(null)
                    .inferredFromSample(false)
                    .notes(notes)
                    .build());
        });
    }

    private <T> AccessResult<T> withClient(Long connectionId, S3Action<T> action) {
        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId);
        if (connOpt.isEmpty()) {
            return AccessResult.notFound("connection.notFound");
        }
        DbConnection conn = connOpt.get();
        try {
            MinioClient client = buildClientFromConnection(conn);
            return action.execute(client);
        } catch (ErrorResponseException e) {
            String code = e.errorResponse().code();
            log.warn("S3 error {}: {}", code, e.getMessage());
            if ("AccessDenied".equals(code) || "SignatureDoesNotMatch".equals(code)) {
                return AccessResult.forbidden("s3.access.forbidden");
            }
            if ("NoSuchBucket".equals(code) || "NoSuchKey".equals(code)) {
                return AccessResult.notFound("s3.access.notFound");
            }
            return AccessResult.error("s3.access.error");
        } catch (Exception e) {
            log.warn("S3 action failed: {}", e.getMessage());
            return AccessResult.error("s3.access.error");
        }
    }

    private MinioClient buildClientFromConnection(DbConnection conn) {
        return buildClient(conn.getHost(), conn.getPort(), conn.getUsername(), conn.getPassword(), conn.isUseHttps());
    }

    MinioClient buildClient(String host, int port, String accessKey, String secretKey, boolean useHttps) {
        String endpoint = resolveEndpoint(host, port, useHttps);
        boolean pathStyle = !host.endsWith(".amazonaws.com");
        return MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey != null ? accessKey : "", secretKey != null ? secretKey : "")
                .build();
    }

    private String resolveEndpoint(String host, int port, boolean useHttps) {
        if (host.startsWith("http://") || host.startsWith("https://")) {
            return host;
        }
        String scheme = useHttps ? "https" : "http";
        boolean standardPort = useHttps ? port == 443 : port == 80;
        return standardPort || port <= 0 ? scheme + "://" + host : scheme + "://" + host + ":" + port;
    }

    private String resolveFormat(String format, String key) {
        if (format != null && !"auto".equalsIgnoreCase(format)) {
            return format.toLowerCase();
        }
        if (key == null) return "text";
        String lower = key.toLowerCase();
        if (lower.endsWith(".json") || lower.endsWith(".jsonl") || lower.endsWith(".ndjson")) return "json";
        if (lower.endsWith(".csv") || lower.endsWith(".tsv")) return "csv";
        if (lower.endsWith(".parquet")) return "parquet";
        return "text";
    }

    private String formatContent(byte[] buf, String format) {
        return switch (format) {
            case "hex" -> formatHex(buf);
            case "json", "csv", "text" -> new String(buf, StandardCharsets.UTF_8);
            case "parquet" -> tryParquetHead(buf);
            default -> new String(buf, StandardCharsets.UTF_8);
        };
    }

    private String formatHex(byte[] buf) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buf.length; i += 16) {
            sb.append(String.format("%08x  ", i));
            for (int j = i; j < Math.min(i + 16, buf.length); j++) {
                sb.append(String.format("%02x ", buf[j]));
                if (j == i + 7) sb.append(" ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private String tryParquetHead(byte[] buf) {
        if (buf.length < 4) return "[too small to be parquet]";
        String magic = new String(buf, 0, 4, StandardCharsets.US_ASCII);
        if (!"PAR1".equals(magic)) {
            return "[not a parquet file - magic bytes: " + HexFormat.of().formatHex(buf, 0, 4) + "]";
        }
        return "[parquet file detected - use query-data to read rows]\nSize: " + buf.length + " bytes read.";
    }

    @FunctionalInterface
    private interface S3Action<T> {
        AccessResult<T> execute(MinioClient client) throws Exception;
    }
}
