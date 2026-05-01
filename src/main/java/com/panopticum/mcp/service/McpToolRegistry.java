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
                        "Required: connectionId (number). Optional: page (default 1), size (default 50, max 100). " +
                        "Postgres/MySQL/MSSQL/ClickHouse/Mongo: databases. Cassandra: keyspaces. Kafka: topics. Redis: databases 0-15. Elasticsearch: indices. Oracle: pseudo-catalog.";
            case "list-namespaces" ->
                "Return schema/namespace level inside a catalog. Required: connectionId (number). Optional: catalog (string), page, size. " +
                        "Postgres/MSSQL/Oracle: returns schemas. Cassandra/MySQL/ClickHouse/Mongo: returns empty with notApplicable=true.";
            case "list-entities" ->
                "Return tables/collections/partitions from which records are read. Required: connectionId (number). Optional: catalog, namespace, page, size. " +
                        "SQL/Cassandra: tables/views. Mongo: collections. Kafka: partitions (catalog=topic). Redis/Elasticsearch: notApplicable.";
            case "query-data" ->
                "Execute a query and return a unified JSON envelope. Required: connectionId (number), query (string). Optional: catalog, namespace, entity, offset, limit (hard max 100). " +
                        "SQL: SQL text. CQL: CQL text. Mongo/Elasticsearch: JSON (MQL/DSL). Kafka: JSON {\"partition\",\"fromOffset\",\"count\",\"fromEnd\"}, catalog=topic. Redis: glob pattern (e.g. user:*), catalog=dbIndex.";
            case "get-record-detail" ->
                "Get full detail of a single record/document for point comparison between sources. " +
                        "Required: connectionId (number), entity (string), and one of: documentId (Mongo), primaryKey (object), locator (engine-specific). " +
                        "Optional: catalog, namespace, queryContext. Use after query-data when comparing specific fields.";
            case "describe-entity" ->
                "Return schema/structure of an entity (table, collection, index, topic, queue, pod). " +
                        "Required: connectionId (number), entity (string — table name, collection name, index name, topic, queue, pod name, etc.). " +
                        "Optional: catalog (database), namespace (schema), sampleSize (Mongo only, default 100). " +
                        "Returns columns, types, PK, FK, indexes, approximate row count. Use before query-data to avoid guessing column names.";
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
                        "size", Map.of("type", "number", "description", "Page size, default 50, max 100")));
                schema.put("required", List.of("connectionId"));
            }
            case "list-namespaces" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID"),
                        "catalog", Map.of("type", "string", "description", "Database/keyspace name if applicable"),
                        "page", Map.of("type", "number", "description", "Page number, default 1"),
                        "size", Map.of("type", "number", "description", "Page size, default 50, max 100")));
                schema.put("required", List.of("connectionId"));
            }
            case "list-entities" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID"),
                        "catalog", Map.of("type", "string", "description", "Database/keyspace"),
                        "namespace", Map.of("type", "string", "description", "Schema name if applicable"),
                        "page", Map.of("type", "number", "description", "Page number, default 1"),
                        "size", Map.of("type", "number", "description", "Page size, default 50, max 100")));
                schema.put("required", List.of("connectionId"));
            }
            case "query-data" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID"),
                        "query", Map.of("type", "string", "description", "SQL/CQL/MQL JSON/ES DSL per dbType; Kafka: JSON opts; Redis: glob pattern"),
                        "catalog", Map.of("type", "string", "description", "Database/keyspace"),
                        "namespace", Map.of("type", "string", "description", "Schema if applicable"),
                        "entity", Map.of("type", "string", "description", "Table/collection for Mongo"),
                        "offset", Map.of("type", "number", "description", "Offset, default 0"),
                        "limit", Map.of("type", "number", "description", "Limit, default 100, hard max 100"),
                        "sort", Map.of("type", "string", "description", "Sort column/field"),
                        "order", Map.of("type", "string", "description", "asc or desc")));
                schema.put("required", List.of("connectionId", "query"));
            }
            case "get-record-detail" -> {
                schema.put("properties", Map.of(
                        "connectionId", Map.of("type", "number", "description", "Connection ID"),
                        "entity", Map.of("type", "string", "description", "Table or collection name"),
                        "documentId", Map.of("type", "string", "description", "Mongo document _id"),
                        "primaryKey", Map.of("type", "object", "description", "Primary key object e.g. {\"id\": 42}"),
                        "locator", Map.of("type", "string", "description", "Engine-specific locator (ctid, rowid)"),
                        "catalog", Map.of("type", "string", "description", "Database/keyspace"),
                        "namespace", Map.of("type", "string", "description", "Schema if applicable"),
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
            default -> {
                schema.put("properties", Map.of());
                schema.put("required", List.of());
            }
        }

        return schema;
    }
}
