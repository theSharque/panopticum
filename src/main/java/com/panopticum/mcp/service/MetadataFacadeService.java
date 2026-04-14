package com.panopticum.mcp.service;

import com.panopticum.core.model.DatabaseInfo;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SchemaInfo;
import com.panopticum.core.model.TableInfo;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.cassandra.model.CassandraKeyspaceInfo;
import com.panopticum.cassandra.model.CassandraTableInfo;
import com.panopticum.cassandra.service.CassandraMetadataService;
import com.panopticum.clickhouse.service.ClickHouseMetadataService;
import com.panopticum.core.model.DbConnection;
import com.panopticum.elasticsearch.model.ElasticsearchIndexInfo;
import com.panopticum.elasticsearch.service.ElasticsearchService;
import com.panopticum.kafka.model.KafkaPartitionInfo;
import com.panopticum.kafka.model.KafkaTopicInfo;
import com.panopticum.kafka.service.KafkaService;
import com.panopticum.kubernetes.model.KubernetesPodInfo;
import com.panopticum.kubernetes.service.KubernetesService;
import com.panopticum.kubernetes.util.KubernetesNamespaceCsv;
import com.panopticum.mongo.model.MongoCollectionInfo;
import com.panopticum.mongo.service.MongoMetadataService;
import com.panopticum.mssql.service.MssqlMetadataService;
import com.panopticum.mysql.service.MySqlMetadataService;
import com.panopticum.oracle.service.OracleMetadataService;
import com.panopticum.postgres.service.PgMetadataService;
import com.panopticum.redis.model.RedisDbInfo;
import com.panopticum.redis.service.RedisMetadataService;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class MetadataFacadeService {

    private static final int MCP_QUERY_HARD_LIMIT = 100;

    private final DbConnectionService dbConnectionService;
    private final PgMetadataService pgMetadataService;
    private final MySqlMetadataService mySqlMetadataService;
    private final MssqlMetadataService mssqlMetadataService;
    private final OracleMetadataService oracleMetadataService;
    private final ClickHouseMetadataService clickHouseMetadataService;
    private final MongoMetadataService mongoMetadataService;
    private final CassandraMetadataService cassandraMetadataService;
    private final KafkaService kafkaService;
    private final RedisMetadataService redisMetadataService;
    private final ElasticsearchService elasticsearchService;
    private final KubernetesService kubernetesService;

    public Optional<DbConnection> getConnection(Long connectionId) {
        return dbConnectionService.findById(connectionId);
    }

    public String getDbType(Long connectionId) {
        return dbConnectionService.findById(connectionId)
                .map(c -> normalizeDbType(c.getType()))
                .orElse(null);
    }

    public Map<String, Object> listCatalogs(Long connectionId, int page, int size, String sort, String order) {
        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId);
        if (connOpt.isEmpty()) {
            return errorResult("connection.notFound");
        }

        String type = normalizeDbType(connOpt.get().getType());

        try {
            return switch (type) {
                case "postgresql" -> toCatalogPage(pgMetadataService.listDatabasesPaged(connectionId, page, size, sort, order), "database");
                case "mysql" -> toCatalogPage(mySqlMetadataService.listDatabasesPaged(connectionId, page, size, sort, order), "database");
                case "sqlserver" -> toCatalogPage(mssqlMetadataService.listDatabasesPaged(connectionId, page, size, sort, order), "database");
                case "clickhouse" -> toCatalogPage(clickHouseMetadataService.listDatabasesPaged(connectionId, page, size, sort, order), "database");
                case "mongodb" -> toCatalogPage(mongoMetadataService.listDatabasesPaged(connectionId, page, size, sort, order), "database");
                case "cassandra" -> toKeyspaceCatalogPage(cassandraMetadataService.listKeyspacesPaged(connectionId, page, size, sort, order));
                case "oracle" -> oraclePseudoCatalog();
                case "kafka" -> toKafkaTopicCatalogPage(kafkaService.listTopicsPaged(connectionId, page, size, sort, order));
                case "redis" -> toRedisDbCatalogPage(redisMetadataService.listDatabasesSorted(connectionId, sort != null ? sort : "dbIndex", order != null ? order : "asc"), page, size);
                case "elasticsearch" -> toElasticsearchIndexCatalogPage(elasticsearchService.listIndicesPaged(connectionId, page, size, sort, order));
                case "kubernetes" -> toKubernetesNamespaceCatalogPage(
                        kubernetesService.listNamespacesPaged(connectionId, page, size, sort, order));
                default -> errorResult("Unsupported dbType: " + type);
            };
        } catch (Exception e) {
            log.warn("listCatalogs failed for connection {}: {}", connectionId, e.getMessage());
            return errorResult(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    public Map<String, Object> listNamespaces(Long connectionId, String catalog, int page, int size, String sort, String order) {
        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId);
        if (connOpt.isEmpty()) {
            return errorResult("connection.notFound");
        }

        String type = normalizeDbType(connOpt.get().getType());

        try {
            return switch (type) {
                case "postgresql" -> toNamespacePage(pgMetadataService.listSchemasPaged(connectionId, catalog != null ? catalog : "", page, size, sort, order), false);
                case "sqlserver" -> toNamespacePage(mssqlMetadataService.listSchemasPaged(connectionId, catalog != null ? catalog : "", page, size, sort, order), false);
                case "oracle" -> toNamespacePage(oracleMetadataService.listSchemasPaged(connectionId, page, size, sort, order), true);
                case "mysql", "clickhouse", "mongodb", "cassandra", "kafka", "redis", "elasticsearch", "kubernetes" -> notApplicableResult();
                default -> errorResult("Unsupported dbType: " + type);
            };
        } catch (Exception e) {
            log.warn("listNamespaces failed for connection {}: {}", connectionId, e.getMessage());
            return errorResult(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    public Map<String, Object> listEntities(Long connectionId, String catalog, String namespace, int page, int size, String sort, String order) {
        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId);
        if (connOpt.isEmpty()) {
            return errorResult("connection.notFound");
        }

        String type = normalizeDbType(connOpt.get().getType());
        String cat = catalog != null && !catalog.isBlank() ? catalog : resolveDefaultCatalog(connOpt.get());
        String ns = namespace != null && !namespace.isBlank() ? namespace : "";

        try {
            return switch (type) {
                case "postgresql" -> toEntityPage(pgMetadataService.listTablesPaged(connectionId, cat, ns, page, size, sort, order), cat, ns);
                case "mysql" -> toEntityPage(mySqlMetadataService.listTablesPaged(connectionId, cat, page, size, sort, order), cat, null);
                case "sqlserver" -> toEntityPage(mssqlMetadataService.listTablesPaged(connectionId, cat, ns, page, size, sort, order), cat, ns);
                case "oracle" -> {
                    String schema = ns.isEmpty() ? (("default".equals(cat) ? "" : cat)) : ns;
                    if (schema.isEmpty()) {
                        schema = resolveDefaultCatalog(connOpt.get());
                    }
                    yield toEntityPage(oracleMetadataService.listTablesPaged(connectionId, schema, page, size, sort, order), null, schema);
                }
                case "clickhouse" -> toEntityPage(clickHouseMetadataService.listTablesPaged(connectionId, cat, page, size, sort, order), cat, null);
                case "mongodb" -> toMongoEntityPage(mongoMetadataService.listCollectionsPaged(connectionId, cat, page, size, sort, order), cat);
                case "cassandra" -> toCassandraEntityPage(cassandraMetadataService.listTablesPaged(connectionId, cat, page, size, sort, order), cat);
                case "kafka" -> cat.isBlank() ? emptyEntityPage("", "") : toKafkaPartitionEntityPage(kafkaService.getPartitions(connectionId, cat), cat, page, size);
                case "kubernetes" -> cat.isBlank()
                        ? errorResult("kubernetes.namespaceRequired")
                        : toKubernetesPodEntityPage(kubernetesService.listPodsPaged(connectionId, cat, page, size, sort, order), cat);
                case "redis", "elasticsearch" -> notApplicableResult();
                default -> errorResult("Unsupported dbType: " + type);
            };
        } catch (Exception e) {
            log.warn("listEntities failed for connection {}: {}", connectionId, e.getMessage());
            return errorResult(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    public Optional<QueryResult> executeQuery(Long connectionId, String catalog, String namespace, String entity,
                                              String query, int offset, int effectiveLimit, String sort, String order) {
        int limit = Math.min(effectiveLimit, MCP_QUERY_HARD_LIMIT);

        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId);
        if (connOpt.isEmpty()) {
            return Optional.empty();
        }

        String type = normalizeDbType(connOpt.get().getType());
        String cat = catalog != null && !catalog.isBlank() ? catalog : resolveDefaultCatalog(connOpt.get());
        String ns = namespace != null && !namespace.isBlank() ? namespace : "";

        try {
            return switch (type) {
                case "postgresql" -> pgMetadataService.executeQuery(connectionId, cat, query, offset, limit, sort != null ? sort : "", order != null ? order : "");
                case "mysql" -> mySqlMetadataService.executeQuery(connectionId, cat, query, offset, limit, sort != null ? sort : "", order != null ? order : "");
                case "sqlserver" -> mssqlMetadataService.executeQuery(connectionId, cat, query, offset, limit, sort != null ? sort : "", order != null ? order : "");
                case "oracle" -> {
                    String schema = ns.isEmpty() ? ("default".equals(cat) ? resolveDefaultCatalog(connOpt.get()) : cat) : ns;
                    yield oracleMetadataService.executeQuery(connectionId, schema, query, offset, limit, sort != null ? sort : "", order != null ? order : "");
                }
                case "clickhouse" -> clickHouseMetadataService.executeQuery(connectionId, cat, query, offset, limit, sort != null ? sort : "", order != null ? order : "");
                case "mongodb" -> mongoMetadataService.executeQuery(connectionId, cat, entity != null ? entity : "", query != null ? query : "{}", offset, limit, sort != null ? sort : "_id", order != null ? order : "asc");
                case "cassandra" -> cassandraMetadataService.executeQuery(connectionId, cat, query, offset, limit);
                case "kafka" -> kafkaService.executeQuery(connectionId, cat, entity, query, offset, limit);
                case "redis" -> redisMetadataService.executeQuery(connectionId, cat, query, limit);
                case "elasticsearch" -> {
                    String index = (entity != null && !entity.isBlank()) ? entity : cat;
                    if (index == null || index.isBlank()) {
                        yield Optional.of(QueryResult.error("catalog or entity (index name) is required for Elasticsearch"));
                    }
                    String dsl = (query != null && !query.isBlank()) ? query.trim() : "{\"query\":{\"match_all\":{}}}";
                    yield elasticsearchService.executeQuery(connectionId, index, dsl, offset, limit);
                }
                case "kubernetes" -> {
                    if (cat == null || cat.isBlank()) {
                        yield Optional.of(QueryResult.error("kubernetes.namespaceRequired"));
                    }
                    if (entity == null || entity.isBlank()) {
                        yield Optional.of(QueryResult.error("kubernetes.podNameRequired"));
                    }
                    yield Optional.of(kubernetesService.tailPodLogsForMcp(connectionId, cat, entity, query, offset, limit));
                }
                default -> Optional.empty();
            };
        } catch (Exception e) {
            log.warn("executeQuery failed for connection {}: {}", connectionId, e.getMessage());
            return Optional.of(QueryResult.error(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    public Optional<Map<String, Object>> getRecordDetail(Long connectionId, String catalog, String namespace, String entity,
                                                         String documentId, Map<String, Object> primaryKey, String locator) {
        Optional<DbConnection> connOpt = dbConnectionService.findById(connectionId);
        if (connOpt.isEmpty()) {
            return Optional.empty();
        }

        String type = normalizeDbType(connOpt.get().getType());
        String cat = catalog != null && !catalog.isBlank() ? catalog : resolveDefaultCatalog(connOpt.get());
        String ns = namespace != null && !namespace.isBlank() ? namespace : "";

        if (documentId != null && !documentId.isBlank() && "mongodb".equals(type)) {
            return mongoMetadataService.getDocument(connectionId, cat, entity, documentId)
                    .map(doc -> {
                        Map<String, Object> out = new HashMap<>();
                        out.put("record", doc);
                        out.put("metadata", Map.of("entity", entity, "dbType", type, "identifierType", "documentId", "identifierValue", documentId));
                        return out;
                    });
        }

        if (primaryKey != null && !primaryKey.isEmpty()) {
            return getRecordByPrimaryKey(connectionId, type, cat, ns, entity, primaryKey);
        }

        if (locator != null && !locator.isBlank() && "postgresql".equals(type)) {
            return getPgRecordByCtid(connectionId, cat, ns, locator);
        }

        return Optional.empty();
    }

    private Optional<Map<String, Object>> getRecordByPrimaryKey(Long connectionId, String type, String catalog, String namespace, String entity, Map<String, Object> primaryKey) {
        return Optional.empty();
    }

    private Optional<Map<String, Object>> getPgRecordByCtid(Long connectionId, String catalog, String namespace, String ctid) {
        return Optional.empty();
    }

    private static String normalizeDbType(String type) {
        if (type == null || type.isBlank()) {
            return "";
        }
        return "mssql".equalsIgnoreCase(type) ? "sqlserver" : type.toLowerCase();
    }

    private static String resolveDefaultCatalog(DbConnection conn) {
        String t = conn.getType() != null ? conn.getType().toLowerCase() : "";
        return switch (t) {
            case "clickhouse" -> "default";
            case "sqlserver" -> "master";
            case "oracle" -> conn.getUsername() != null && !conn.getUsername().isBlank() ? conn.getUsername() : "";
            case "kubernetes" -> {
                List<String> nss = KubernetesNamespaceCsv.parse(conn.getDbName());
                yield nss.isEmpty() ? "" : nss.get(0);
            }
            default -> conn.getDbName() != null ? conn.getDbName() : "";
        };
    }

    private Map<String, Object> toCatalogPage(Page<DatabaseInfo> page, String kind) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (DatabaseInfo db : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", db.getName());
            m.put("kind", kind);
            m.put("sizeOnDisk", db.getSizeOnDisk());
            m.put("sizeOnDiskFormatted", db.getSizeOnDiskFormatted());
            items.add(m);
        }
        return successCatalogPage(items, page);
    }

    private Map<String, Object> toKeyspaceCatalogPage(Page<CassandraKeyspaceInfo> page) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (CassandraKeyspaceInfo ks : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", ks.getName());
            m.put("kind", "keyspace");
            m.put("durableWrites", ks.getDurableWrites());
            m.put("replicationFormatted", ks.getReplicationFormatted());
            items.add(m);
        }
        return successCatalogPage(items, page);
    }

    private Map<String, Object> oraclePseudoCatalog() {
        List<Map<String, Object>> items = List.of(Map.of("name", "default", "kind", "catalog", "description", "Oracle uses schemas; use list-namespaces to get schemas"));
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("pagination", Map.of("page", 1, "size", 1, "hasMore", false, "fromRow", 1, "toRow", 1));
        return out;
    }

    private Map<String, Object> successCatalogPage(List<Map<String, Object>> items, Page<?> page) {
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("pagination", Map.of(
                "page", page.getPage(),
                "size", page.getSize(),
                "hasMore", page.isHasMore(),
                "fromRow", page.getFromRow(),
                "toRow", page.getToRow()));
        return out;
    }

    private Map<String, Object> toNamespacePage(Page<SchemaInfo> page, boolean oracle) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (SchemaInfo s : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", s.getName());
            m.put("kind", oracle ? "schema" : "namespace");
            m.put("owner", s.getOwner());
            m.put("tableCount", s.getTableCount());
            items.add(m);
        }
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("notApplicable", false);
        out.put("pagination", Map.of(
                "page", page.getPage(),
                "size", page.getSize(),
                "hasMore", page.isHasMore()));
        return out;
    }

    private Map<String, Object> notApplicableResult() {
        Map<String, Object> out = new HashMap<>();
        out.put("items", List.of());
        out.put("notApplicable", true);
        return out;
    }

    private Map<String, Object> emptyEntityPage(String catalog, String namespace) {
        Map<String, Object> out = new HashMap<>();
        out.put("items", List.of());
        out.put("scope", Map.of("catalog", catalog != null ? catalog : "", "namespace", namespace != null ? namespace : ""));
        out.put("pagination", Map.of("page", 1, "size", 50, "hasMore", false));
        return out;
    }

    private Map<String, Object> toEntityPage(Page<TableInfo> page, String catalog, String namespace) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (TableInfo t : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", t.getName());
            m.put("kind", t.getType() != null && t.getType().toLowerCase().contains("view") ? "view" : "table");
            m.put("approximateRowCount", t.getApproximateRowCount());
            m.put("sizeOnDisk", t.getSizeOnDisk());
            m.put("sizeOnDiskFormatted", t.getSizeOnDiskFormatted());
            items.add(m);
        }
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("scope", Map.of("catalog", catalog != null ? catalog : "", "namespace", namespace != null ? namespace : ""));
        out.put("pagination", Map.of("page", page.getPage(), "size", page.getSize(), "hasMore", page.isHasMore()));
        return out;
    }

    private Map<String, Object> toMongoEntityPage(Page<MongoCollectionInfo> page, String catalog) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (MongoCollectionInfo c : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", c.getName());
            m.put("kind", "collection");
            m.put("documentCount", c.getDocumentCount());
            m.put("sizeOnDisk", c.getSizeOnDisk());
            m.put("sizeOnDiskFormatted", c.getSizeOnDiskFormatted());
            items.add(m);
        }
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("scope", Map.of("catalog", catalog != null ? catalog : "", "namespace", ""));
        out.put("pagination", Map.of("page", page.getPage(), "size", page.getSize(), "hasMore", page.isHasMore()));
        return out;
    }

    private Map<String, Object> toCassandraEntityPage(Page<CassandraTableInfo> page, String catalog) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (CassandraTableInfo t : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", t.getName());
            m.put("kind", "table");
            m.put("type", t.getType());
            m.put("comment", t.getComment());
            items.add(m);
        }
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("scope", Map.of("catalog", catalog != null ? catalog : "", "namespace", ""));
        out.put("pagination", Map.of("page", page.getPage(), "size", page.getSize(), "hasMore", page.isHasMore()));
        return out;
    }

    private Map<String, Object> toKafkaTopicCatalogPage(Page<KafkaTopicInfo> page) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (KafkaTopicInfo t : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", t.getName());
            m.put("kind", "topic");
            m.put("partitionCount", t.getPartitionCount());
            items.add(m);
        }
        return successCatalogPage(items, page);
    }

    private Map<String, Object> toRedisDbCatalogPage(List<RedisDbInfo> dbs, int page, int size) {
        int limit = Math.min(Math.max(1, size), 100);
        int offset = Math.max(0, (page - 1) * limit);
        List<Map<String, Object>> items = new ArrayList<>();
        for (int i = offset; i < Math.min(offset + limit, dbs.size()); i++) {
            RedisDbInfo db = dbs.get(i);
            Map<String, Object> m = new HashMap<>();
            m.put("name", String.valueOf(db.getDbIndex()));
            m.put("kind", "database");
            m.put("keyCount", db.getKeyCount());
            items.add(m);
        }
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("pagination", Map.of(
                "page", page,
                "size", limit,
                "hasMore", offset + items.size() < dbs.size(),
                "fromRow", dbs.isEmpty() ? 0 : offset + 1,
                "toRow", offset + items.size()));
        return out;
    }

    private Map<String, Object> toElasticsearchIndexCatalogPage(Page<ElasticsearchIndexInfo> page) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (ElasticsearchIndexInfo idx : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", idx.getIndex());
            m.put("kind", "index");
            m.put("docsCount", idx.getDocsCount());
            m.put("storeSize", idx.getStoreSize());
            items.add(m);
        }
        return successCatalogPage(items, page);
    }

    private Map<String, Object> toKafkaPartitionEntityPage(List<KafkaPartitionInfo> partitions, String catalog, int page, int size) {
        List<KafkaPartitionInfo> sorted = partitions.stream()
                .sorted((a, b) -> Integer.compare(a.getPartition(), b.getPartition()))
                .toList();
        int limit = Math.min(Math.max(1, size), 100);
        int offset = Math.max(0, (page - 1) * limit);
        List<Map<String, Object>> items = new ArrayList<>();
        for (int i = offset; i < Math.min(offset + limit, sorted.size()); i++) {
            KafkaPartitionInfo p = sorted.get(i);
            Map<String, Object> m = new HashMap<>();
            m.put("name", String.valueOf(p.getPartition()));
            m.put("kind", "partition");
            m.put("partition", p.getPartition());
            items.add(m);
        }
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("scope", Map.of("catalog", catalog != null ? catalog : "", "namespace", ""));
        out.put("pagination", Map.of(
                "page", page,
                "size", limit,
                "hasMore", offset + items.size() < sorted.size()));
        return out;
    }

    private Map<String, Object> toKubernetesNamespaceCatalogPage(Page<String> page) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (String ns : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", ns);
            m.put("kind", "namespace");
            items.add(m);
        }
        return successCatalogPage(items, page);
    }

    private Map<String, Object> toKubernetesPodEntityPage(Page<KubernetesPodInfo> page, String namespace) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (KubernetesPodInfo p : page.getItems()) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", p.getName());
            m.put("kind", "pod");
            m.put("phase", p.getPhase());
            items.add(m);
        }
        Map<String, Object> out = new HashMap<>();
        out.put("items", items);
        out.put("scope", Map.of("catalog", namespace != null ? namespace : "", "namespace", ""));
        out.put("pagination", Map.of(
                "page", page.getPage(),
                "size", page.getSize(),
                "hasMore", page.isHasMore()));
        return out;
    }

    private Map<String, Object> errorResult(String message) {
        Map<String, Object> out = new HashMap<>();
        out.put("error", message);
        return out;
    }

    public int getQueryHardLimit() {
        return MCP_QUERY_HARD_LIMIT;
    }
}
