package com.panopticum.core.util;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import lombok.experimental.UtilityClass;

@UtilityClass
public class AdminLockGuard {

    public void assertNotLocked(boolean adminLock) {
        if (adminLock) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "admin.lock.enabled");
        }
    }
}
