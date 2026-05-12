package com.panopticum.couchbase.service;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.ClusterOptions;
import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.couchbase.model.CouchbaseBucketInfo;
import com.panopticum.couchbase.model.CouchbaseScopeCollections;
import com.panopticum.mcp.model.ColumnInfo;
import com.panopticum.mcp.model.EntityDescription;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class CouchbaseService {

    private static final Duration READY = Duration.ofSeconds(20);

    private final DbConnectionService dbConnectionService;

    public Optional<String> testProbe(String host, int port, String database, String username, String password, boolean useHttps) {
        if (host == null || host.isBlank()) {
            return Optional.of("error.specifyHost");
        }
        if (username == null || username.isBlank()) {
            return Optional.of("error.specifyHostDbUser");
        }
        DbConnection probe = DbConnection.builder()
                .type("couchbase")
                .host(host.trim())
                .port(port > 0 ? port : 11210)
                .dbName(database != null ? database : "")
                .username(username.trim())
                .password(password != null ? password : "")
                .useHttps(useHttps)
                .build();
        return testWithConnection(probe);
    }

    public Optional<String> testSaved(Long connectionId) {
        Optional<DbConnection> c = dbConnectionService.findById(connectionId);
        if (c.isEmpty()) {
            return Optional.of("connection.notFound");
        }
        if (!"couchbase".equalsIgnoreCase(c.get().getType())) {
            return Optional.of("error.connectionNotAvailable");
        }
        return testWithConnection(c.get());
    }

    private Optional<String> testWithConnection(DbConnection c) {
        try (Cluster cluster = connect(c)) {
            cluster.waitUntilReady(READY);
            cluster.ping();
            return Optional.empty();
        } catch (Exception e) {
            log.warn("Couchbase ping failed: {}", e.getMessage());
            return Optional.ofNullable(e.getMessage());
        }
    }

    public Cluster connect(DbConnection c) {
        String connStr = connectionString(c);
        ClusterOptions opts = ClusterOptions.clusterOptions(c.getUsername(),
                c.getPassword() != null ? c.getPassword() : "");
        return Cluster.connect(connStr, opts);
    }

    public static String connectionString(DbConnection c) {
        String hostField = c.getHost() != null ? c.getHost().trim() : "";
        if (hostField.contains("://")) {
            return hostField;
        }
        String scheme = c.isUseHttps() ? "couchbases" : "couchbase";
        int port = c.getPort() > 0 ? c.getPort() : 11210;
        return scheme + "://" + hostField + ":" + port;
    }

    public List<CouchbaseBucketInfo> listBucketInfos(Long connectionId) {
        DbConnection c = requireConn(connectionId);
        try (Cluster cluster = connect(c)) {
            cluster.waitUntilReady(READY);
            Map<String, BucketSettings> all = cluster.buckets().getAllBuckets();
            List<CouchbaseBucketInfo> out = new ArrayList<>();
            for (BucketSettings bs : all.values()) {
                out.add(CouchbaseBucketInfo.builder()
                        .name(bs.name())
                        .bucketType(String.valueOf(bs.bucketType()))
                        .ramQuotaMb(bs.ramQuotaMB())
                        .replicaCount(bs.numReplicas())
                        .build());
            }
            out.sort(Comparator.comparing(CouchbaseBucketInfo::getName, String.CASE_INSENSITIVE_ORDER));
            return out;
        }
    }

    public Page<CouchbaseBucketInfo> listBucketsPaged(Long connectionId, int page, int size, String sort, String order) {
        List<CouchbaseBucketInfo> all = new ArrayList<>(listBucketInfos(connectionId));
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        Comparator<CouchbaseBucketInfo> cmp;
        if ("size".equals(sortBy)) {
            cmp = desc
                    ? (a, b) -> Long.compare(b.getRamQuotaMb(), a.getRamQuotaMb())
                    : Comparator.comparingLong(CouchbaseBucketInfo::getRamQuotaMb);
        } else {
            cmp = desc
                    ? (a, b) -> b.getName().compareToIgnoreCase(a.getName())
                    : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        }
        all.sort(cmp);
        return Page.of(all, page, size, sortBy, order != null ? order : "asc");
    }

    public Page<DatabaseInfo> listBucketsAsDatabasePage(Long connectionId, int page, int size, String sort, String order) {
        Page<CouchbaseBucketInfo> p = listBucketsPaged(connectionId, page, size, sort, order);
        List<DatabaseInfo> items = p.getItems().stream()
                .map(b -> new DatabaseInfo(b.getName(), 0L, ""))
                .toList();
        return new Page<>(items, p.getPage(), p.getSize(), p.getSort(), p.getOrder(),
                p.getFromRow(), p.getToRow(), p.isHasPrev(), p.isHasMore(), p.getPrevOffset(), p.getNextOffset());
    }

    public List<CouchbaseScopeCollections> listScopesAndCollections(Long connectionId, String bucketName) {
        DbConnection c = requireConn(connectionId);
        try (Cluster cluster = connect(c)) {
            cluster.waitUntilReady(READY);
            Bucket bucket = cluster.bucket(bucketName);
            bucket.waitUntilReady(READY);
            List<CouchbaseScopeCollections> out = new ArrayList<>();
            for (ScopeSpec scope : bucket.collections().getAllScopes()) {
                List<String> cols = new ArrayList<>();
                for (CollectionSpec cs : scope.collections()) {
                    cols.add(cs.name());
                }
                cols.sort(String.CASE_INSENSITIVE_ORDER);
                out.add(CouchbaseScopeCollections.builder()
                        .scopeName(scope.name())
                        .collectionNames(cols)
                        .build());
            }
            out.sort(Comparator.comparing(CouchbaseScopeCollections::getScopeName, String.CASE_INSENSITIVE_ORDER));
            return out;
        }
    }

    public Page<SchemaInfo> listScopesPaged(Long connectionId, String bucketName, int page, int size, String sort, String order) {
        List<SchemaInfo> rows = new ArrayList<>();
        for (CouchbaseScopeCollections sc : listScopesAndCollections(connectionId, bucketName)) {
            rows.add(new SchemaInfo(sc.getScopeName(), "", sc.getCollectionNames().size()));
        }
        boolean desc = "desc".equalsIgnoreCase(order);
        String sortBy = sort != null ? sort : "name";
        Comparator<SchemaInfo> cmp = "tables".equals(sortBy)
                ? (desc ? (a, b) -> Integer.compare(b.getTableCount(), a.getTableCount()) : Comparator.comparingInt(SchemaInfo::getTableCount))
                : (desc ? (a, b) -> b.getName().compareToIgnoreCase(a.getName()) : (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        rows.sort(cmp);
        return Page.of(rows, page, size, sortBy, order != null ? order : "asc");
    }

    public Page<TableInfo> listCollectionsAsTablesPaged(Long connectionId, String bucket, String scope, int page, int size,
                                                        String sort, String order) {
        List<CouchbaseScopeCollections> tree = listScopesAndCollections(connectionId, bucket);
        List<String> collections = tree.stream()
                .filter(s -> s.getScopeName().equals(scope))
                .findFirst()
                .map(CouchbaseScopeCollections::getCollectionNames)
                .orElse(List.of());
        List<TableInfo> tables = new ArrayList<>();
        for (String n : collections) {
            tables.add(new TableInfo(n, "collection", 0L, 0L, ""));
        }
        boolean desc = "desc".equalsIgnoreCase(order);
        Comparator<TableInfo> cmp = desc
                ? (a, b) -> b.getName().compareToIgnoreCase(a.getName())
                : (a, b) -> a.getName().compareToIgnoreCase(b.getName());
        tables.sort(cmp);
        return Page.of(tables, page, size, sort != null ? sort : "name", order != null ? order : "asc");
    }

    public com.panopticum.core.model.QueryResult scanCollection(Long connectionId, String bucket, String scope, String collection,
                                                               int offset, int limit) {
        DbConnection c = requireConn(connectionId);
        String bq = bt(bucket);
        String sq = bt(scope);
        String cq = bt(collection);
        String stmt = "SELECT META(b).id AS documentId, b.* FROM " + bq + "." + sq + "." + cq + " AS b LIMIT " + Math.max(1, limit)
                + " OFFSET " + Math.max(0, offset);
        try (Cluster cluster = connect(c)) {
            cluster.waitUntilReady(READY);
            QueryResult qr = cluster.query(stmt, QueryOptions.queryOptions().timeout(Duration.ofMinutes(2)));
            List<JsonObject> objs = qr.rowsAsObject();
            LinkedHashSet<String> colNames = new LinkedHashSet<>();
            colNames.add("documentId");
            List<Map<String, Object>> decoded = new ArrayList<>();
            for (JsonObject jo : objs) {
                Map<String, Object> row = jsonObjectToMap(jo);
                decoded.add(row);
                colNames.addAll(row.keySet());
            }
            List<String> columns = new ArrayList<>(colNames);
            List<List<Object>> rows = new ArrayList<>();
            for (Map<String, Object> row : decoded) {
                List<Object> line = new ArrayList<>();
                for (String col : columns) {
                    line.add(row.get(col));
                }
                rows.add(line);
            }
            List<String> types = columns.stream().map(x -> "json").toList();
            boolean hasMore = objs.size() == limit;
            return new com.panopticum.core.model.QueryResult(columns, types, rows, null, null, offset, limit, hasMore);
        } catch (Exception e) {
            log.warn("scanCollection failed: {}", e.getMessage());
            return com.panopticum.core.model.QueryResult.error(e.getMessage() != null ? e.getMessage() : "error.queryExecutionFailed");
        }
    }

    public com.panopticum.core.model.QueryResult executeN1ql(Long connectionId, String statement, int offset, int limit) {
        DbConnection c = requireConn(connectionId);
        try (Cluster cluster = connect(c)) {
            cluster.waitUntilReady(READY);
            String stmt = statement.strip().replaceFirst(";+\\s*$", "");
            String windowed = wrapLimitOffset(stmt, offset, limit);
            QueryResult qr = cluster.query(windowed, QueryOptions.queryOptions().timeout(Duration.ofMinutes(2)));
            List<JsonObject> objs = qr.rowsAsObject();
            if (objs.isEmpty()) {
                return new com.panopticum.core.model.QueryResult(List.of(), List.of(), List.of(), null, null, offset, limit, false);
            }
            LinkedHashSet<String> colNames = new LinkedHashSet<>();
            for (JsonObject jo : objs) {
                colNames.addAll(jo.getNames());
            }
            List<String> columns = new ArrayList<>(colNames);
            List<List<Object>> rows = new ArrayList<>();
            for (JsonObject jo : objs) {
                List<Object> line = new ArrayList<>();
                for (String col : columns) {
                    line.add(jsonValueToJava(jo.get(col)));
                }
                rows.add(line);
            }
            List<String> types = columns.stream().map(x -> "json").toList();
            boolean hasMore = objs.size() == limit;
            return new com.panopticum.core.model.QueryResult(columns, types, rows, null, null, offset, limit, hasMore);
        } catch (Exception e) {
            log.warn("executeN1ql failed: {}", e.getMessage());
            return com.panopticum.core.model.QueryResult.error(e.getMessage() != null ? e.getMessage() : "error.queryExecutionFailed");
        }
    }

    private static String wrapLimitOffset(String stmt, int offset, int limit) {
        int lim = Math.max(1, limit);
        int off = Math.max(0, offset);
        String upper = stmt.toUpperCase().trim();
        if (upper.contains(" LIMIT ") || upper.contains("\nLIMIT ")) {
            return stmt;
        }
        return stmt + " LIMIT " + lim + " OFFSET " + off;
    }

    public Optional<Map<String, Object>> getDocument(Long connectionId, String bucket, String scope, String collection, String documentId) {
        if (documentId == null || documentId.isBlank()) {
            return Optional.empty();
        }
        DbConnection c = requireConn(connectionId);
        try (Cluster cluster = connect(c)) {
            cluster.waitUntilReady(READY);
            Collection coll = cluster.bucket(bucket).scope(scope).collection(collection);
            GetResult gr = coll.get(documentId);
            JsonObject jo = gr.contentAsObject();
            return Optional.of(jsonObjectToMap(jo));
        } catch (DocumentNotFoundException e) {
            return Optional.empty();
        } catch (Exception e) {
            log.warn("getDocument failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public Optional<String> replaceDocument(Long connectionId, String bucket, String scope, String collection,
                                            String documentId, String jsonString) {
        if (documentId == null || documentId.isBlank() || bucket == null || bucket.isBlank()
                || scope == null || scope.isBlank() || collection == null || collection.isBlank()
                || jsonString == null) {
            return Optional.of("error.specifyCollection");
        }
        DbConnection c = requireConn(connectionId);
        try (Cluster cluster = connect(c)) {
            cluster.waitUntilReady(READY);
            Collection coll = cluster.bucket(bucket).scope(scope).collection(collection);
            coll.replace(documentId, JsonObject.fromJson(jsonString));
            return Optional.empty();
        } catch (DocumentNotFoundException e) {
            return Optional.of("Document not found");
        } catch (Exception e) {
            log.warn("replaceDocument failed: {}", e.getMessage());
            return Optional.of(e.getMessage() != null ? e.getMessage() : "error.queryExecutionFailed");
        }
    }

    public Optional<EntityDescription> describeCollection(Long connectionId, String bucket, String scope, String collection) {
        DbConnection c = requireConn(connectionId);
        String bq = bt(bucket);
        String sq = bt(scope);
        String cq = bt(collection);
        String stmt = "SELECT b.* FROM " + bq + "." + sq + "." + cq + " AS b LIMIT 50";
        try (Cluster cluster = connect(c)) {
            cluster.waitUntilReady(READY);
            QueryResult qr = cluster.query(stmt, QueryOptions.queryOptions().timeout(Duration.ofMinutes(1)));
            TreeSet<String> keys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            int rowCount = 0;
            for (JsonObject jo : qr.rowsAsObject()) {
                rowCount++;
                keys.addAll(jo.getNames());
            }
            List<ColumnInfo> cols = new ArrayList<>();
            int pos = 1;
            for (String k : keys) {
                cols.add(ColumnInfo.builder()
                        .name(k)
                        .type("json")
                        .nullable(true)
                        .primaryKey(false)
                        .position(pos++)
                        .build());
            }
            List<String> pk = List.of();
            return Optional.of(EntityDescription.builder()
                    .connectionId(null)
                    .dbType(null)
                    .entityKind("collection")
                    .catalog(bucket)
                    .namespace(scope)
                    .entity(collection)
                    .columns(cols)
                    .primaryKey(pk)
                    .foreignKeys(List.of())
                    .indexes(List.of())
                    .approximateRowCount((long) rowCount)
                    .inferredFromSample(true)
                    .notes(List.of())
                    .build());
        } catch (Exception e) {
            log.warn("describeCollection failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private DbConnection requireConn(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .filter(c -> "couchbase".equalsIgnoreCase(c.getType()))
                .orElseThrow(() -> new IllegalArgumentException("connection.notFound"));
    }

    private static String bt(String id) {
        if (id == null) {
            return "``";
        }
        return "`" + id.replace("`", "``") + "`";
    }

    private static Map<String, Object> jsonObjectToMap(JsonObject jo) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (String name : jo.getNames()) {
            m.put(name, jsonValueToJava(jo.get(name)));
        }
        return m;
    }

    private static Object jsonValueToJava(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof JsonObject o) {
            return jsonObjectToMap(o);
        }
        if (v instanceof JsonArray arr) {
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < arr.size(); i++) {
                list.add(jsonValueToJava(arr.get(i)));
            }
            return list;
        }
        return v;
    }
}
