FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

COPY gradle gradle
COPY gradlew build.gradle settings.gradle ./
COPY src src/

RUN ./gradlew shadowJar --no-daemon

FROM eclipse-temurin:17-jre-alpine

ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION

RUN apk update \
    && apk upgrade --no-cache \
    && apk del --no-cache \
        gnupg \
        gpg \
        gpg-agent \
        gpgsm \
        dirmngr \
        fontconfig \
        ttf-dejavu \
        openssl \
        coreutils \
        coreutils-env \
        coreutils-fmt \
        coreutils-sha512sum \
    && addgroup -g 1001 app \
    && adduser -u 1001 -G app -s /sbin/nologin -D app

WORKDIR /app

ENV PANOPTICUM_DB_PATH=/data/panopticum

RUN mkdir -p /data && chown -R 1001:1001 /data

COPY --from=builder --chown=1001:1001 /app/build/libs/panopticum-all.jar app.jar

USER 1001

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD ["/bin/sh", "-c", "{ printf 'GET /actuator/health/liveness HTTP/1.0\\r\\n\\r\\n'; sleep 0.1; } | nc -w 3 127.0.0.1 8080 | grep -q 'HTTP/1.[01] 200'"]

ENTRYPOINT ["java", "-jar", "app.jar"]
