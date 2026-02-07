package com.panopticum.auth;

import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.security.authentication.AuthenticationFailureReason;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.provider.AuthenticationProvider;
import jakarta.inject.Singleton;

@Singleton
public class BasicAuthProvider implements AuthenticationProvider {

    private final String expectedUsername;
    private final String expectedPassword;

    public BasicAuthProvider(
            @Value("${PANOPTICUM_USER:admin}") String expectedUsername,
            @Value("${PANOPTICUM_PASSWORD:admin}") String expectedPassword) {
        this.expectedUsername = expectedUsername;
        this.expectedPassword = expectedPassword;
    }

    @Override
    public AuthenticationResponse authenticate(@Nullable Object httpRequest, @NonNull AuthenticationRequest authenticationRequest) {
        String identity = (String) authenticationRequest.getIdentity();
        String secret = (String) authenticationRequest.getSecret();
        if (expectedUsername.equals(identity) && expectedPassword.equals(secret)) {
            return AuthenticationResponse.success(identity);
        }
        return AuthenticationResponse.failure(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH);
    }
}
