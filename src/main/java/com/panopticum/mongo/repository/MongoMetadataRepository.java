package com.panopticum.mongo.repository;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.util.SizeFormatter;
import com.panopticum.mongo.model.MongoCollectionInfo;
import com.panopticum.mongo.model.MongoDatabaseInfo;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class MongoMetadataRepository {

    private static final String MONGO_PREFIX = "mongodb://";
    private static final int DEFAULT_PORT = 27017;

    private final DbConnectionService dbConnectionService;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    @Value("${panopticum.limits.collections:500}")
    private int collectionsLimit;

    public Optional<MongoClient> createClient(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .filter(c -> "mongodb".equalsIgnoreCase(c.getType()))
                .flatMap(this::createClientFromConnection);
    }

    public String buildConnectionString(String host, int port, String dbName, String username, String password) {
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

    private Optional<MongoClient> createClientFromConnection(DbConnection conn) {
        try {
            String uri = buildConnectionString(conn.getHost(), conn.getPort(),
                    conn.getDbName() != null ? conn.getDbName() : "",
                    conn.getUsername() != null ? conn.getUsername() : "",
                    conn.getPassword() != null ? conn.getPassword() : "");
            return Optional.of(MongoClients.create(uri));
        } catch (Exception e) {
            log.warn("Failed to connect to {}: {}", conn.getName(), e.getMessage());
            return Optional.empty();
        }
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
                infos.add(new MongoDatabaseInfo(name, sizeOnDisk, SizeFormatter.formatSize(sizeOnDisk)));
            }
            return infos;
        } catch (Exception e) {
            log.warn("listDatabaseInfos failed: {}", e.getMessage());
            return List.of();
        }
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
                    infos.add(new MongoCollectionInfo(name, count, size, SizeFormatter.formatSize(size)));
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

    public List<Document> findDocuments(Long connectionId, String dbName, String collectionName,
                                        Bson filter, int offset, int limit, String sortField, int sortDirection) {
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null || dbName == null || dbName.isBlank() || collectionName == null || collectionName.isBlank()) {
                return List.of();
            }
            MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
            int lim = Math.min(limit, queryRowsLimit);
            String sortKey = sortField != null && !sortField.isBlank() ? sortField : "_id";
            int dir = (sortDirection == -1) ? -1 : 1;
            return collection.find(filter).sort(new Document(sortKey, dir)).skip(offset).limit(lim + 1).into(new ArrayList<>());
        } catch (Exception e) {
            log.warn("findDocuments failed: {}", e.getMessage());
            return List.of();
        }
    }

    public List<Document> findAllDocuments(Long connectionId, String dbName, String collectionName,
                                           int offset, int limit, String sortField, int sortDirection) {
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null || dbName == null || dbName.isBlank() || collectionName == null || collectionName.isBlank()) {
                return List.of();
            }
            MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
            int lim = Math.min(limit, queryRowsLimit);
            String sortKey = sortField != null && !sortField.isBlank() ? sortField : "_id";
            int dir = (sortDirection == -1) ? -1 : 1;
            return collection.find().sort(new Document(sortKey, dir)).skip(offset).limit(lim + 1).into(new ArrayList<>());
        } catch (Exception e) {
            log.warn("findAllDocuments failed: {}", e.getMessage());
            return List.of();
        }
    }

    public List<Document> aggregateDocuments(Long connectionId, String dbName, String collectionName,
                                             List<Bson> pipeline, int offset, int limit) {
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null || dbName == null || dbName.isBlank() || collectionName == null || collectionName.isBlank()) {
                return List.of();
            }
            MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
            List<Bson> withLimit = new ArrayList<>(pipeline);
            withLimit.add(new Document("$skip", offset));
            withLimit.add(new Document("$limit", limit + 1));
            return collection.aggregate(withLimit).into(new ArrayList<>());
        } catch (Exception e) {
            log.warn("aggregateDocuments failed: {}", e.getMessage());
            return List.of();
        }
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

    public Optional<String> replaceDocument(Long connectionId, String dbName, String collectionName,
                                           String docId, Document doc) {
        if (docId == null || docId.isBlank() || collectionName == null || collectionName.isBlank()
                || dbName == null || dbName.isBlank() || doc == null) {
            return Optional.of("error.specifyCollection");
        }
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null) {
                return Optional.of("error.connectionNotAvailable");
            }
            Object filterId = docId.length() == 24 && docId.matches("[0-9a-fA-F]+") ? new ObjectId(docId) : docId;
            MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
            collection.replaceOne(new Document("_id", filterId), doc);
            return Optional.empty();
        } catch (Exception e) {
            log.warn("replaceDocument failed: {}", e.getMessage());
            return Optional.of(e.getMessage());
        }
    }
}
