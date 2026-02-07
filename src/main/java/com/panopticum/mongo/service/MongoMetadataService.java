package com.panopticum.mongo.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.util.StringUtils;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.mongo.model.MongoCollectionInfo;
import com.panopticum.mongo.model.MongoDatabaseInfo;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
@Slf4j
public class MongoMetadataService {

    private static final String MONGO_PREFIX = "mongodb://";
    private static final int DEFAULT_PORT = 27017;

    private final DbConnectionService dbConnectionService;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    @Value("${panopticum.limits.collections:500}")
    private int collectionsLimit;

    public MongoMetadataService(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    public Optional<String> testConnection(String host, int port, String dbName, String username, String password) {
        if (host == null || host.isBlank()) {
            return Optional.of("Укажите хост");
        }

        String uri = buildConnectionString(host.trim(), port, dbName != null ? dbName.trim() : "", username != null ? username.trim() : "", password != null ? password : "");

        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase(dbName != null && !dbName.isBlank() ? dbName : "admin").runCommand(new Document("ping", 1));

            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    private Optional<MongoClient> createClient(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .filter(c -> "mongodb".equalsIgnoreCase(c.getType()))
                .flatMap(this::createClientFromConnection);
    }

    private Optional<MongoClient> createClientFromConnection(DbConnection conn) {
        try {
            String uri = buildConnectionString(conn.getHost(), conn.getPort(), conn.getDbName() != null ? conn.getDbName() : "",
                    conn.getUsername() != null ? conn.getUsername() : "", conn.getPassword() != null ? conn.getPassword() : "");

            return Optional.of(MongoClients.create(uri));
        } catch (Exception e) {
            log.warn("Failed to connect to {}: {}", conn.getName(), e.getMessage());
            return Optional.empty();
        }
    }

    private String buildConnectionString(String host, int port, String dbName, String username, String password) {
        int p = port > 0 ? port : DEFAULT_PORT;
        StringBuilder sb = new StringBuilder(MONGO_PREFIX);
        if (username != null && !username.isBlank()) {
            sb.append(URLEncoder.encode(username, StandardCharsets.UTF_8));

            if (password != null && !password.isBlank()) {
                sb.append(':').append(URLEncoder.encode(password, StandardCharsets.UTF_8));
            }
            sb.append('@');
        }

        sb.append(host).append(':').append(p);
        if (dbName != null && !dbName.isBlank()) {
            sb.append('/').append(dbName);
        }

        return sb.toString();
    }

    public List<String> listDatabases(Long connectionId) {
        return listDatabaseInfos(connectionId).stream().map(MongoDatabaseInfo::getName).collect(Collectors.toList());
    }

    public List<MongoDatabaseInfo> listDatabaseInfos(Long connectionId) {
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null) {
                return List.of();
            }

            List<MongoDatabaseInfo> infos = new ArrayList<>();
            for (Document doc : client.listDatabases()) {
                String name = doc.getString("name");
                long sizeOnDisk = doc.getLong("sizeOnDisk") != null ? doc.getLong("sizeOnDisk") : 0L;
                infos.add(new MongoDatabaseInfo(name, sizeOnDisk, formatSize(sizeOnDisk)));
            }
            return infos;
        } catch (Exception e) {
            log.warn("listDatabaseInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    private static String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    public List<String> listCollections(Long connectionId, String dbName, int offset, int limit) {
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null || dbName == null || dbName.isBlank()) {
                return List.of();
            }

            MongoDatabase database = client.getDatabase(dbName);
            List<String> all = StreamSupport.stream(database.listCollectionNames().spliterator(), false).collect(Collectors.toList());
            int maxItems = Math.min(limit, Math.max(0, collectionsLimit - offset));
            int end = Math.min(offset + maxItems, all.size());

            return offset < all.size() ? all.subList(offset, end) : List.of();
        } catch (Exception e) {
            log.warn("listCollections failed: {}", e.getMessage());
            return List.of();
        }
    }

    public List<MongoCollectionInfo> listCollectionInfos(Long connectionId, String dbName, List<String> collectionNames) {
        if (collectionNames == null || collectionNames.isEmpty()) {
            return List.of();
        }
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null || dbName == null || dbName.isBlank()) {
                return List.of();
            }

            MongoDatabase database = client.getDatabase(dbName);
            List<MongoCollectionInfo> infos = new ArrayList<>();
            for (String name : collectionNames) {
                try {
                    Document stats = database.runCommand(new Document("collStats", name));
                    Number countNum = (Number) stats.get("count");
                    Number sizeNum = (Number) stats.get("size");
                    long count = countNum != null ? countNum.longValue() : 0L;
                    long size = sizeNum != null ? sizeNum.longValue() : 0L;
                    infos.add(new MongoCollectionInfo(name, count, size, formatSize(size)));
                } catch (Exception e) {
                    log.debug("collStats for {} failed: {}", name, e.getMessage());
                    infos.add(new MongoCollectionInfo(name, 0L, 0L, "—"));
                }
            }
            return infos;
        } catch (Exception e) {
            log.warn("listCollectionInfos failed: {}", e.getMessage());
            return List.of();
        }
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String dbName, String collectionName, String queryText,
                                              int offset, int limit) {
        if (collectionName == null || collectionName.isBlank()) {
            return Optional.of(QueryResult.error("Укажите коллекцию"));
        }
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null) {
                return Optional.of(QueryResult.error("Connection not available"));
            }

            if (dbName == null || dbName.isBlank()) {
                return Optional.of(QueryResult.error("Укажите базу данных"));
            }

            MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
            String trimmed = queryText != null ? queryText.trim() : "";
            int lim = limit > 0 ? Math.min(limit, queryRowsLimit) : Math.min(100, queryRowsLimit);
            int off = Math.max(0, offset);

            if (trimmed.isEmpty() || "{}".equals(trimmed)) {
                List<Document> docs = collection.find().skip(off).limit(lim + 1).into(new ArrayList<>());
                return Optional.of(documentsToQueryResult(docs, lim, off));
            }

            if (trimmed.startsWith("[")) {
                return executeAggregate(collection, trimmed, off, lim);
            }

            return executeFind(collection, trimmed, off, lim);
        } catch (Exception e) {
            log.warn("executeQuery failed: {}", e.getMessage());
            return Optional.of(QueryResult.error(e.getMessage()));
        }
    }

    private Optional<QueryResult> executeFind(MongoCollection<Document> collection, String queryText, int offset, int limit) {
        try {
            Bson filter = Document.parse(queryText);
            List<Document> docs = collection.find(filter).skip(offset).limit(limit + 1).into(new ArrayList<>());

            return Optional.of(documentsToQueryResult(docs, limit, offset));
        } catch (Exception e) {
            return Optional.of(QueryResult.error(e.getMessage()));
        }
    }

    private Optional<QueryResult> executeAggregate(MongoCollection<Document> collection, String queryText, int offset, int limit) {
        try {
            List<Bson> pipeline = parsePipeline(queryText);
            if (pipeline.isEmpty()) {
                return Optional.of(QueryResult.error("Неверный формат pipeline (ожидается массив этапов)"));
            }

            List<Bson> withLimit = new ArrayList<>(pipeline);
            withLimit.add(new Document("$skip", offset));
            withLimit.add(new Document("$limit", limit + 1));
            List<Document> docs = collection.aggregate(withLimit).into(new ArrayList<>());

            return Optional.of(documentsToQueryResult(docs, limit, offset));
        } catch (Exception e) {
            return Optional.of(QueryResult.error(e.getMessage()));
        }
    }

    private List<Bson> parsePipeline(String json) {
        String trimmed = json.trim();
        if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
            return List.of();
        }

        List<Bson> result = new ArrayList<>();
        String inner = trimmed.substring(1, trimmed.length() - 1).trim();

        if (inner.isEmpty()) {
            return result;
        }

        int depth = 0;
        int start = 0;

        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '{') {
                if (depth == 0) {
                    start = i;
                }
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    result.add(Document.parse(inner.substring(start, i + 1)));
                }
            }
        }

        return result;
    }

    private QueryResult documentsToQueryResult(List<Document> docs, int limit, int offset) {
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        for (Document doc : docs) {
            columns.addAll(doc.keySet());
        }

        List<String> columnList = new ArrayList<>(columns);
        boolean hasMore = docs.size() > limit;
        List<Document> rowsDocs = hasMore ? docs.subList(0, limit) : docs;
        List<List<Object>> rows = new ArrayList<>();
        List<String> docIds = new ArrayList<>();

        for (Document doc : rowsDocs) {
            List<Object> row = new ArrayList<>();

            for (String col : columnList) {
                row.add(StringUtils.truncateCell(doc.get(col)));
            }

            rows.add(row);
            Object idVal = doc.get("_id");
            docIds.add(idVal instanceof ObjectId ? ((ObjectId) idVal).toHexString() : (idVal != null ? idVal.toString() : ""));
        }

        return new QueryResult(columnList, null, rows, docIds, null, offset, limit, hasMore);
    }

    public Optional<Document> getDocument(Long connectionId, String dbName, String collectionName, String docId) {
        if (docId == null || docId.isBlank() || collectionName == null || collectionName.isBlank()
                || dbName == null || dbName.isBlank()) {
            return Optional.empty();
        }
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null) {
                return Optional.empty();
            }
            MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
            Document doc;
            if (docId.length() == 24 && docId.matches("[0-9a-fA-F]+")) {
                doc = collection.find(new Document("_id", new ObjectId(docId))).first();
            } else {
                doc = collection.find(new Document("_id", docId)).first();
            }
            return Optional.ofNullable(doc);
        } catch (Exception e) {
            log.warn("getDocument failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public String documentToPrettyJson(Document doc) {
        if (doc == null) {
            return "{}";
        }
        return doc.toJson(JsonWriterSettings.builder().indent(true).build());
    }
}
