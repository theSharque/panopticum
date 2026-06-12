package com.panopticum.core.controller;

import com.panopticum.core.error.ApiErrors;
import com.panopticum.core.model.QueryResult;
import com.panopticum.core.model.SqlQueryRequest;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.core.sql.SqlStatementClassifier;
import com.panopticum.core.util.ApiQueryParams;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;

import java.util.Optional;

public abstract class AbstractConnectionApiController {

    @FunctionalInterface
    protected interface SqlQueryRunner {
        Optional<QueryResult> run(Long id, String dbName, String sql,
                int offset, int limit, String sort, String order, String search);
    }

    @FunctionalInterface
    protected interface CqlQueryRunner {
        Optional<QueryResult> run(Long id, String keyspaceName, String cql, int offset, int limit);
    }

    @FunctionalInterface
    protected interface N1qlRunner {
        QueryResult run(Long id, String sql, int offset, int limit);
    }

    protected final DbConnectionService dbConnectionService;

    @Value("${panopticum.read-only:false}")
    protected boolean readOnly;

    protected AbstractConnectionApiController(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    protected void ensureConnectionExists(Long id) {
        if (dbConnectionService.findById(id).isEmpty()) {
            throw new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound");
        }
    }

    protected void assertNotReadOnly() {
        if (readOnly) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "read.only.enabled");
        }
    }

    protected void assertNotReadOnlyForSqlMutation(String sql) {
        if (readOnly && sql != null && SqlStatementClassifier.isMutation(sql)) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "read.only.enabled");
        }
    }

    protected QueryResult runSqlQuery(Long id, SqlQueryRequest request, SqlQueryRunner runner) {
        return runSqlQuery(id, request.getSql(), request.getDbName(), request.getOffset(), request.getLimit(),
                request.getSort(), request.getOrder(), request.getSearch(), runner);
    }

    protected QueryResult runSqlQuery(Long id, String sql, String dbName, Integer offset, Integer limit,
                                      String sort, String order, String search, SqlQueryRunner runner) {
        ensureConnectionExists(id);
        if (sql == null || sql.isBlank()) {
            return QueryResult.error(ApiErrors.EMPTY_QUERY);
        }
        assertNotReadOnlyForSqlMutation(sql);
        int off = ApiQueryParams.normalizedOffset(offset);
        int lim = ApiQueryParams.normalizedLimit(limit);
        String searchTerm = ApiQueryParams.trimmedSearchOrEmpty(search);
        String sortVal = sort != null ? sort : "";
        String orderVal = order != null ? order : "";
        return runner.run(id, dbName, sql, off, lim, sortVal, orderVal, searchTerm)
                .orElse(QueryResult.error(ApiErrors.QUERY_EXECUTION_FAILED));
    }

    protected QueryResult runCqlQuery(Long id, String keyspaceName, String cql, Integer offset, Integer limit,
                                      CqlQueryRunner runner) {
        ensureConnectionExists(id);
        if (cql == null || cql.isBlank()) {
            return QueryResult.error(ApiErrors.EMPTY_QUERY);
        }
        assertNotReadOnlyForSqlMutation(cql);
        int off = ApiQueryParams.normalizedOffset(offset);
        int lim = ApiQueryParams.normalizedLimit(limit);
        return runner.run(id, keyspaceName, cql, off, lim)
                .orElse(QueryResult.error(ApiErrors.QUERY_EXECUTION_FAILED));
    }

    protected QueryResult runN1qlQuery(Long id, SqlQueryRequest request, N1qlRunner runner) {
        ensureConnectionExists(id);
        if (request.getSql() == null || request.getSql().isBlank()) {
            return QueryResult.error(ApiErrors.EMPTY_QUERY);
        }
        int offset = ApiQueryParams.normalizedOffset(request.getOffset());
        int limit = ApiQueryParams.normalizedLimit(request.getLimit());
        return runner.run(id, request.getSql(), offset, limit);
    }
}
