package com.panopticum.core.controller;

import com.panopticum.core.service.DbConnectionService;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;

public abstract class AbstractConnectionApiController {

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
}
