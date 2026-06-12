package com.panopticum.core.controller;

import com.panopticum.core.service.DbConnectionService;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;

public abstract class AbstractConnectionUiController {

    protected final DbConnectionService dbConnectionService;

    @Value("${panopticum.read-only:false}")
    protected boolean readOnly;

    protected AbstractConnectionUiController(DbConnectionService dbConnectionService) {
        this.dbConnectionService = dbConnectionService;
    }

    protected void assertNotReadOnly() {
        if (readOnly) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "read.only.enabled");
        }
    }
}
