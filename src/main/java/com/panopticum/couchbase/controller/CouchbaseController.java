package com.panopticum.couchbase.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.panopticum.couchbase.model.CouchbaseBucketInfo;
import com.panopticum.couchbase.model.CouchbaseScopeCollections;
import com.panopticum.couchbase.service.CouchbaseService;
import com.panopticum.core.model.BreadcrumbItem;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.model.Page;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.ui.AppAlerts;
import com.panopticum.core.util.ControllerModelHelper;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.ModelAndView;
import io.micronaut.views.View;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller("/couchbase")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
public class CouchbaseController {

    private final DbConnectionService dbConnectionService;
    private final CouchbaseService couchbaseService;
    private final ObjectMapper objectMapper;

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/buckets")
    @View("couchbase/buckets")
    public Map<String, Object> buckets(@PathVariable Long id,
                                        @QueryValue(value = "page", defaultValue = "1") int page,
                                        @QueryValue(value = "size", defaultValue = "50") int size,
                                        @QueryValue(value = "sort", defaultValue = "name") String sort,
                                        @QueryValue(value = "order", defaultValue = "asc") String order) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("itemUrlPrefix", "/couchbase/" + id + "/");

        Page<CouchbaseBucketInfo> paged = couchbaseService.listBucketsPaged(id, page, size, sort, order);
        ControllerModelHelper.addPagination(model, paged, "items");
        ControllerModelHelper.addOrderToggles(model, paged.getSort(), paged.getOrder(),
                Map.of("name", "orderName", "size", "orderSize"));

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{bucket}/collections")
    @View("couchbase/collections")
    public Map<String, Object> collections(@PathVariable Long id, @PathVariable String bucket) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/couchbase/" + id + "/buckets"));
        breadcrumbs.add(new BreadcrumbItem(bucket, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("bucket", bucket);

        List<CouchbaseScopeCollections> tree = couchbaseService.listScopesAndCollections(id, bucket);
        model.put("scopes", tree);

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{bucket}/{scope}/{collection}")
    @View("couchbase/documents")
    public Map<String, Object> documents(@PathVariable Long id, @PathVariable String bucket,
                                        @PathVariable String scope, @PathVariable String collection,
                                        @QueryValue(value = "offset", defaultValue = "0") int offset,
                                        @QueryValue(value = "limit", defaultValue = "50") int limit) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/couchbase/" + id + "/buckets"));
        breadcrumbs.add(new BreadcrumbItem(bucket, "/couchbase/" + id + "/" + bucket + "/collections"));
        breadcrumbs.add(new BreadcrumbItem(scope + " / " + collection, null));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("bucket", bucket);
        model.put("scope", scope);
        model.put("collection", collection);
        model.put("offset", offset);
        model.put("limit", limit);

        QueryResult qr = couchbaseService.scanCollection(id, bucket, scope, collection, offset, limit);
        AppAlerts.fromControllerMessage(model, qr.hasError() ? qr.getError() : null);
        model.put("columns", qr.getColumns());
        model.put("columnTypes", qr.getColumnTypes() != null ? qr.getColumnTypes() : List.<String>of());
        model.put("rows", qr.getRows());
        model.put("hasPrev", qr.hasPrev());
        model.put("hasMore", qr.isHasMore());
        model.put("nextOffset", qr.nextOffset());
        model.put("prevOffset", qr.prevOffset());
        model.put("fromRow", qr.fromRow());
        model.put("toRow", qr.toRow());

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/{bucket}/{scope}/{collection}/detail")
    @View("couchbase/document-detail")
    public Map<String, Object> documentDetail(@PathVariable Long id, @PathVariable String bucket,
                                              @PathVariable String scope, @PathVariable String collection,
                                              @QueryValue String documentId) {
        return documentDetailModel(id, bucket, scope, collection, documentId, null);
    }

    @Post("/{id}/{bucket}/{scope}/{collection}/detail")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object saveDocument(@PathVariable Long id, @PathVariable String bucket, @PathVariable String scope,
                               @PathVariable String collection, String documentId, String body) {
        Optional<String> err = couchbaseService.replaceDocument(id, bucket, scope, collection, documentId, body);

        if (err.isPresent()) {
            Map<String, Object> model = documentDetailModel(id, bucket, scope, collection, documentId, body);
            AppAlerts.fromControllerMessage(model, err.get());

            return new ModelAndView<>("couchbase/document-detail", model);
        }

        String q = documentId != null && !documentId.isBlank()
                ? "?documentId=" + URLEncoder.encode(documentId, StandardCharsets.UTF_8)
                : "";
        return HttpResponse.redirect(URI.create("/couchbase/" + id + "/" + bucket + "/" + scope + "/" + collection + "/detail" + q));
    }

    private Map<String, Object> documentDetailModel(Long id, String bucket, String scope, String collection,
                                                    String documentId, String fallbackBody) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            model.put("prettyJson", fallbackBody != null ? fallbackBody : "{}");

            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/couchbase/" + id + "/buckets"));
        breadcrumbs.add(new BreadcrumbItem(bucket, "/couchbase/" + id + "/" + bucket + "/collections"));
        breadcrumbs.add(new BreadcrumbItem(collection, "/couchbase/" + id + "/" + bucket + "/" + scope + "/" + collection));
        breadcrumbs.add(new BreadcrumbItem("document", null, false));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("bucket", bucket);
        model.put("scope", scope);
        model.put("collection", collection);
        model.put("documentId", documentId);

        Optional<Map<String, Object>> document = fallbackBody == null
                ? couchbaseService.getDocument(id, bucket, scope, collection, documentId)
                : Optional.empty();
        String json = fallbackBody != null ? fallbackBody : document.map(this::documentToPrettyJson).orElse("{}");
        model.put("prettyJson", json);
        addDataDiffPayload(model, conn.get(), id, bucket, scope, collection, documentId, json);
        if (fallbackBody == null && document.isEmpty()) {
            AppAlerts.raw(model, "Document not found");
        }

        return model;
    }

    private String documentToPrettyJson(Map<String, Object> doc) {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(doc);
        } catch (JsonProcessingException e) {
            return String.valueOf(doc);
        }
    }

    private void addDataDiffPayload(Map<String, Object> model, DbConnection conn, Long id, String bucket,
                                    String scope, String collection, String documentId, String json) {
        String label = conn.getName() + " / " + bucket + " / " + scope + " / " + collection + " / "
                + (documentId != null ? documentId : "");
        try {
            Map<String, Object> payload = Map.of(
                    "source", "couchbase",
                    "connectionId", id,
                    "connectionName", conn.getName(),
                    "label", label,
                    "data", json,
                    "dataFormat", "json"
            );
            model.put("dataDiffPayload", objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            model.put("dataDiffPayload", (String) null);
        }
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/{id}/query")
    @View("couchbase/query")
    public Map<String, Object> queryGet(@PathVariable Long id,
                                        @QueryValue(value = "statement", defaultValue = "") String statement,
                                        @QueryValue(value = "offset", defaultValue = "0") int offset,
                                        @QueryValue(value = "limit", defaultValue = "100") int limit) {
        return queryModel(id, statement, offset, limit);
    }

    @Produces(MediaType.TEXT_HTML)
    @Post("/{id}/query")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @View("couchbase/query")
    public Map<String, Object> queryPost(@PathVariable Long id, @Nullable String statement,
                                         @Nullable Integer offset, @Nullable Integer limit) {
        return queryModel(id, statement != null ? statement : "", offset != null ? offset : 0, limit != null ? limit : 100);
    }

    private Map<String, Object> queryModel(Long id, String statement, int offset, int limit) {
        Map<String, Object> model = ControllerModelHelper.baseModel(id, dbConnectionService);
        Optional<DbConnection> conn = dbConnectionService.findById(id);
        if (conn.isEmpty()) {
            return model;
        }

        List<BreadcrumbItem> breadcrumbs = new ArrayList<>();
        breadcrumbs.add(new BreadcrumbItem(conn.get().getName(), "/couchbase/" + id + "/buckets"));
        breadcrumbs.add(new BreadcrumbItem("N1QL", null, false));
        ControllerModelHelper.addBreadcrumbs(model, breadcrumbs);
        model.put("connectionId", id);
        model.put("statement", statement);
        model.put("offset", offset);
        model.put("limit", limit);

        if (statement != null && !statement.isBlank()) {
            QueryResult qr = couchbaseService.executeN1ql(id, statement, offset, limit);
            AppAlerts.fromControllerMessage(model, qr.hasError() ? qr.getError() : null);
            model.put("columns", qr.getColumns());
            model.put("columnTypes", qr.getColumnTypes() != null ? qr.getColumnTypes() : List.<String>of());
            model.put("rows", qr.getRows());
            model.put("hasPrev", qr.hasPrev());
            model.put("hasMore", qr.isHasMore());
            model.put("nextOffset", qr.nextOffset());
            model.put("prevOffset", qr.prevOffset());
            model.put("fromRow", qr.fromRow());
            model.put("toRow", qr.toRow());
        } else {
            model.put("columns", List.<String>of());
            model.put("columnTypes", List.<String>of());
            model.put("rows", List.<List<Object>>of());
            model.put("hasPrev", false);
            model.put("hasMore", false);
            model.put("nextOffset", limit);
            model.put("prevOffset", 0);
            model.put("fromRow", 0);
            model.put("toRow", 0);
        }

        return model;
    }
}
