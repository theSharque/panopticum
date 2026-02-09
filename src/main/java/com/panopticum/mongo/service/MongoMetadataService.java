package com.panopticum.mongo.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.util.StringUtils;
import com.panopticum.mongo.model.MongoCollectionInfo;
import com.panopticum.mongo.model.MongoDatabaseInfo;
import com.panopticum.mongo.repository.MongoMetadataRepository;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class MongoMetadataService {

    private final MongoMetadataRepository mongoMetadataRepository;

    @Value("${panopticum.limits.query-rows:1000}")
    private int queryRowsLimit;

    public Optional<String> testConnection(String host, int port, String dbName, String username, String password) {
        if (host == null || host.isBlank()) {
            return Optional.of("error.specifyHost");
        }
        String uri = mongoMetadataRepository.buildConnectionString(
                host.trim(), port, dbName != null ? dbName.trim() : "",
                username != null ? username.trim() : "", password != null ? password : "");
        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase(dbName != null && !dbName.isBlank() ? dbName : "admin")
                    .runCommand(new Document("ping", 1));
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public List<String> listDatabases(Long connectionId) {
        return mongoMetadataRepository.listDatabaseInfos(connectionId).stream()
                .map(MongoDatabaseInfo::getName).collect(Collectors.toList());
    }

    public List<MongoDatabaseInfo> listDatabaseInfos(Long connectionId) {
        return mongoMetadataRepository.listDatabaseInfos(connectionId);
    }

    public Page<MongoDatabaseInfo> listDatabasesPaged(Long connectionId, int page, int size, String sort, String order) {
        List<MongoDatabaseInfo> all = new ArrayList<>(listDatabaseInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<MongoDatabaseInfo> comparator = "size".equals(sortBy)
                ? (desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk()))
                : (desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        List<MongoDatabaseInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public List<String> listCollections(Long connectionId, String dbName, int offset, int limit) {
        return mongoMetadataRepository.listCollections(connectionId, dbName, offset, limit);
    }

    public List<MongoCollectionInfo> listCollectionInfos(Long connectionId, String dbName, List<String> collectionNames) {
        return mongoMetadataRepository.listCollectionInfos(connectionId, dbName, collectionNames);
    }

    public Page<MongoCollectionInfo> listCollectionsPaged(Long connectionId, String dbName, int page, int size, String sort, String order) {
        List<String> allNames = mongoMetadataRepository.listCollections(connectionId, dbName, 0, 500);
        List<MongoCollectionInfo> all = mongoMetadataRepository.listCollectionInfos(connectionId, dbName, allNames);
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        java.util.Comparator<MongoCollectionInfo> comparator;
        if ("size".equals(sortBy)) {
            comparator = desc ? (a, b) -> Long.compare(b.getSizeOnDisk(), a.getSizeOnDisk()) : (a, b) -> Long.compare(a.getSizeOnDisk(), b.getSizeOnDisk());
        } else if ("count".equals(sortBy)) {
            comparator = desc ? (a, b) -> Long.compare(b.getDocumentCount(), a.getDocumentCount()) : (a, b) -> Long.compare(a.getDocumentCount(), b.getDocumentCount());
        } else {
            comparator = desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        List<MongoCollectionInfo> sorted = all.stream().sorted(comparator).toList();
        return Page.of(sorted, page, size, sortBy, order != null ? order : "asc");
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String dbName, String collectionName, String queryText,
                                              int offset, int limit, String sort, String order) {
        if (collectionName == null || collectionName.isBlank()) {
            return Optional.of(QueryResult.error("error.specifyCollection"));
        }
        if (mongoMetadataRepository.createClient(connectionId).isEmpty()) {
            return Optional.of(QueryResult.error("error.connectionNotAvailable"));
        }
        if (dbName == null || dbName.isBlank()) {
            return Optional.of(QueryResult.error("error.specifyDatabase"));
        }
        String trimmed = queryText != null ? queryText.trim() : "";
        int lim = limit > 0 ? Math.min(limit, queryRowsLimit) : Math.min(100, queryRowsLimit);
        int off = Math.max(0, offset);
        String sortField = sort != null && !sort.isBlank() ? sort : "_id";
        int sortDirection = "desc".equalsIgnoreCase(order) ? -1 : 1;

        if (trimmed.isEmpty() || "{}".equals(trimmed)) {
            List<Document> docs = mongoMetadataRepository.findAllDocuments(connectionId, dbName, collectionName, off, lim, sortField, sortDirection);
            return Optional.of(documentsToQueryResult(docs, lim, off));
        }
        if (trimmed.startsWith("[")) {
            List<Bson> pipeline = parsePipeline(trimmed);
            if (pipeline.isEmpty()) {
                return Optional.of(QueryResult.error("error.invalidPipelineFormat"));
            }
            List<Document> docs = mongoMetadataRepository.aggregateDocuments(connectionId, dbName, collectionName, pipeline, off, lim);
            return Optional.of(documentsToQueryResult(docs, lim, off));
        }
        try {
            Bson filter = Document.parse(trimmed);
            List<Document> docs = mongoMetadataRepository.findDocuments(connectionId, dbName, collectionName, filter, off, lim, sortField, sortDirection);
            return Optional.of(documentsToQueryResult(docs, lim, off));
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
        return mongoMetadataRepository.getDocument(connectionId, dbName, collectionName, docId);
    }

    public String documentToPrettyJson(Document doc) {
        if (doc == null) {
            return "{}";
        }
        return doc.toJson(JsonWriterSettings.builder().indent(true).build());
    }

    public Optional<String> replaceDocument(Long connectionId, String dbName, String collectionName, String docId, String jsonString) {
        if (docId == null || docId.isBlank() || collectionName == null || collectionName.isBlank()
                || dbName == null || dbName.isBlank() || jsonString == null) {
            return Optional.of("error.specifyCollection");
        }
        try {
            Document doc = Document.parse(jsonString);
            Object idVal = docId.length() == 24 && docId.matches("[0-9a-fA-F]+") ? new ObjectId(docId) : docId;
            doc.put("_id", idVal);
            return mongoMetadataRepository.replaceDocument(connectionId, dbName, collectionName, docId, doc);
        } catch (org.bson.json.JsonParseException e) {
            return Optional.of(e.getMessage());
        }
    }
}
