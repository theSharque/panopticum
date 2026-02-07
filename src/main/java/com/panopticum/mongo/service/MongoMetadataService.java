package com.panopticum.mongo.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;

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
        try (MongoClient client = createClient(connectionId).orElse(null)) {
            if (client == null) {
                return List.of();
            }
            return StreamSupport.stream(client.listDatabaseNames().spliterator(), false).collect(Collectors.toList());
        } catch (Exception e) {
            log.warn("listDatabases failed: {}", e.getMessage());
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
        for (Document doc : rowsDocs) {
            List<Object> row = new ArrayList<>();
            for (String col : columnList) {
                row.add(doc.get(col));
            }
            rows.add(row);
        }
        return new QueryResult(columnList, rows, null, offset, limit, hasMore);
    }
}
