package com.panopticum.mcp.service;

import com.panopticum.mcp.model.McpTool;
import com.panopticum.mcp.model.McpToolRequest;
import com.panopticum.mcp.model.McpToolResponse;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class McpToolRegistry {

    private final List<McpToolExecutor> toolExecutors;
    private Map<String, McpToolExecutor> executorMap;
    private List<McpTool> registeredTools;

    private void initialize() {
        if (executorMap == null) {
            executorMap = new HashMap<>();
            for (McpToolExecutor executor : toolExecutors) {
                executorMap.put(executor.getToolName(), executor);
                log.debug("Registered MCP tool: {}", executor.getToolName());
            }
            registeredTools = createToolDefinitions();
        }
    }

    public List<McpTool> getAllTools() {
        initialize();
        return registeredTools;
    }

    public McpToolResponse executeTool(McpToolRequest request) {
        initialize();

        McpToolExecutor executor = executorMap.get(request.getName());
        if (executor == null) {
            log.warn("Tool not found: {}", request.getName());
            return McpToolResponse.builder()
                    .isError(true)
                    .error("Tool not found: " + request.getName())
                    .build();
        }

        try {
            return executor.execute(request);
        } catch (Exception e) {
            log.error("Error executing tool {}: {}", request.getName(), e.getMessage(), e);
            return McpToolResponse.builder()
                    .isError(true)
                    .error("Tool execution failed: " + e.getMessage())
                    .build();
        }
    }

    private List<McpTool> createToolDefinitions() {
        return executorMap.values().stream()
                .map(executor -> {
                    String toolName = executor.getToolName();
                    return McpTool.builder()
                            .name(toolName)
                            .description(getToolDescription(toolName))
                            .inputSchema(getToolInputSchema(toolName))
                            .build();
                })
                .toList();
    }

    private String getToolDescription(String toolName) {
        return switch (toolName) {
            case "list-data-sources" ->
                "Return a safe list of data sources available for MCP queries. No credentials, host, port, or connection strings are returned. " +
                        "Each item includes: connectionId, name, dbType, queryFormat, hierarchyModel. " +
                        "Arguments: pass empty object {}.";
            case "list-catalogs" ->
                "Return the top-level structure (databases/keyspaces/topics) for a given connection. " +
                        "Required: connectionId (number). Optional: page (default 1), size (default 50, max 100), sort, order. " +
                        "Postgres/MySQL/MSSQL/ClickHouse/Mongo: databases. Couchbase: buckets. Cassandra: keyspaces. Kafka: topics. Redis: databases 0-15. Elasticsearch: indices. Kubernetes: namespaces. S3: buckets. Prometheus: jobs. RabbitMQ: vhosts. Oracle: pseudo-catalog.";
            case "list-namespaces" ->
                "Return schema/namespace level inside a catalog. Required: connectionId (number). Optional: catalog (string), page, size. " +
                        "Postgres/MSSQL/Oracle: returns schemas. Couchbase: scopes. Cassandra/MySQL/ClickHouse/Mongo/Kafka/Redis/Elasticsearch/Kubernetes/S3/Prometheus/RabbitMQ: returns empty with notApplicable=true.";
            case "list-entities" ->
                "Return tables/collections/partitions/objects/queues from which records are read. Required: connectionId (number). Optional: catalog, namespace, page, size, sort, order. " +
                        "SQL/Cassandra: tables/views. Mongo: collections. Couchbase: collections (catalog=bucket, namespace=scope). Kafka: partitions (catalog=topic). Kubernetes: pods (catalog=namespace). S3: objects (catalog=bucket, namespace=prefix). Prometheus: metrics (catalog=job). RabbitMQ: queues (catalog=vhost). Redis/Elasticsearch: notApplicable.";
            case "query-data" ->
                "Execute a query and return a unified JSON envelope. Required: connectionId (number), query (string) unless publish is set. Optional: catalog, namespace, entity, offset, limit (hard max 100), publish (array of message payloads for RabbitMQ). " +
                        "SQL/CQL: SELECT returns rows; DML (INSERT/UPDATE/DELETE) returns rows_affected column; SQL with RETURNING returns rows. N1QL: N1QL text (Couchbase). Mongo/Elasticsearch: JSON (MQL/DSL). Kafka: JSON {\"partition\",\"fromOffset\",\"count\",\"fromEnd\"}, catalog=topic. Redis: glob pattern (e.g. user:*), catalog=dbIndex. S3: JSON {\"headBytes\",\"format\"}, catalog=bucket, entity=key. Prometheus: PromQL or JSON range query. RabbitMQ peek: count or JSON {\"count\"}, catalog=vhost, entity=queue; publish: publish array, catalog=vhost, entity=queue.";
            case "get-record-detail" ->
                "Get full detail of a single record/document for point comparison between sources. " +
                        "Required: connectionId (number), entity (string), and an identifier. DocumentId for Mongo/Couchbase; entity as key name for Redis (catalog=dbIndex, default 0). primaryKey and locator reserved for engine-specific point lookup. " +
                        "Optional: catalog, namespace, queryContext. Use after query-data when comparing specific fields.";
            case "describe-entity" ->
                "Return schema/structure of an entity (table, collection, index, topic, queue, pod). " +
                        "Required: connectionId (number), entity (string — table name, collection name, index name, topic, queue, pod name, etc.). " +
                        "Optional: catalog (database/keyspace/bucket/vhost/job), namespace (schema/scope/prefix), sampleSize (Mongo only, default 100). " +
                        "Returns columns, types, PK, FK, indexes, approximate row count. Use before query-data to avoid guessing column names.";
            case "resolve-panopticum-link" ->
                "Resolve a Panopticum UI URL or path into connectionId and MCP scope (catalog, namespace, entity). " +
                        "Required: link (string) — e.g. http://host/postgres/5/mydb/public/users or /postgres/5/mydb or connection-name/mydb from breadcrumbs. " +
                        "On failure returns error and availablePaths: configured paths from app storage only (/type/id and /type/id/db_name when set in connection settings). " +
                        "Use before other tools when the user pasted a link from the UI; avoids list-data-sources.";
            default -> "MCP tool: " + toolName;
        };
    }

    private Map<String, Object> getToolInputSchema(String toolName) {
        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "object");

        switch (toolName) {
            case "list-data-sources" -> {
                schema.put("properties", Map.of());
                schema.put("required", List.of());
            }
            case "list-catalogs" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID from list-data-sources"),
                        "page", Map.of("type", "number", "description", "Page number, default 1"),
                        "size", Map.of("type", "number", "description", "Page size, default 50, max 100"),
                        "sort", Map.of("type", "string", "description", "Sort field"),
                        "order", Map.of("type", "string", "description", "asc or desc")));
                schema.put("required", List.of("connectionId"));
            }
            case "list-namespaces" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID"),
                        "catalog", Map.of("type", "string", "description", "Database/keyspace name if applicable"),
                        "page", Map.of("type", "number", "description", "Page number, default 1"),
                        "size", Map.of("type", "number", "description", "Page size, default 50, max 100"),
                        "sort", Map.of("type", "string", "description", "Sort field"),
                        "order", Map.of("type", "string", "description", "asc or desc")));
                schema.put("required", List.of("connectionId"));
            }
            case "list-entities" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID"),
                        "catalog", Map.of("type", "string", "description", "Database/keyspace/bucket/topic/vhost/job"),
                        "namespace", Map.of("type", "string", "description", "Schema/scope/prefix if applicable"),
                        "page", Map.of("type", "number", "description", "Page number, default 1"),
                        "size", Map.of("type", "number", "description", "Page size, default 50, max 100"),
                        "sort", Map.of("type", "string", "description", "Sort field"),
                        "order", Map.of("type", "string", "description", "asc or desc")));
                schema.put("required", List.of("connectionId"));
            }
            case "query-data" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID"),
                        "query", Map.of("type", "string", "description", "SQL/CQL/N1QL/MQL JSON/ES DSL per dbType; Kafka: JSON opts; Redis: glob pattern; S3: JSON opts; Prometheus: PromQL; RabbitMQ peek: count or JSON count"),
                        "publish", Map.of("type", "array", "items", Map.of("type", "string"), "description", "RabbitMQ: message payloads to publish (catalog=vhost, entity=queue)"),
                        "catalog", Map.of("type", "string", "description", "Database/keyspace/bucket/topic/vhost/job"),
                        "namespace", Map.of("type", "string", "description", "Schema/scope/prefix if applicable"),
                        "entity", Map.of("type", "string", "description", "Table/collection/index/object key/queue"),
                        "offset", Map.of("type", "number", "description", "Offset, default 0"),
                        "limit", Map.of("type", "number", "description", "Limit, default 100, hard max 100"),
                        "sort", Map.of("type", "string", "description", "Sort column/field"),
                        "order", Map.of("type", "string", "description", "asc or desc")));
                schema.put("required", List.of("connectionId"));
            }
            case "get-record-detail" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID"),
                        "entity", Map.of("type", "string", "description", "Table/collection name, or Redis key name"),
                        "documentId", Map.of("type", "string", "description", "Mongo _id or Couchbase document ID"),
                        "primaryKey", Map.of("type", "object", "description", "Reserved primary key object e.g. {\"id\": 42}"),
                        "locator", Map.of("type", "string", "description", "Reserved engine-specific locator (ctid, rowid)"),
                        "catalog", Map.of("type", "string", "description", "Database/keyspace/bucket"),
                        "namespace", Map.of("type", "string", "description", "Schema/scope if applicable"),
                        "queryContext", Map.of("type", "string", "description", "Original SQL/CQL/MQL if detail from query result")));
                schema.put("required", List.of("connectionId", "entity"));
            }
            case "describe-entity" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID"),
                        "entity", Map.of("type", "string", "description", "Table/collection/index/topic/queue/pod name"),
                        "catalog", Map.of("type", "string", "description", "Database/keyspace/bucket"),
                        "namespace", Map.of("type", "string", "description", "Schema if applicable"),
                        "sampleSize", Map.of("type", "number", "description", "Sample size for schema inference (Mongo, default 100, max 500)")));
                schema.put("required", List.of("connectionId", "entity"));
            }
            case "resolve-panopticum-link" -> {
                schema.put("properties", Map.of(
                        "link", Map.of("type", "string", "description", "Panopticum UI URL or path from copy/link")));
                schema.put("required", List.of("link"));
            }
            default -> {
                schema.put("properties", Map.of());
                schema.put("required", List.of());
            }
        }

        return schema;
    }
}
