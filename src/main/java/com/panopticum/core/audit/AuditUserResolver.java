package com.panopticum.core.audit;

import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

@Singleton
@RequiredArgsConstructor
public class AuditUserResolver {

    private static final String ANONYMOUS = "anonymous";

    private final SecurityService securityService;

    public String currentUser() {
        return securityService.getAuthentication()
                .map(auth -> auth.getName())
                .orElse(ANONYMOUS);
    }
}
