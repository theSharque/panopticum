FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

COPY gradle gradle
COPY gradlew build.gradle settings.gradle ./
COPY src src/

RUN ./gradlew shadowJar --no-daemon

FROM eclipse-temurin:17-jre-alpine

ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION

RUN apk add --no-cache bash \
    && addgroup -g 1001 app \
    && adduser -u 1001 -G app -s /sbin/nologin -D app

WORKDIR /app

ENV PANOPTICUM_DB_PATH=/data/panopticum

RUN mkdir -p /data && chown -R 1001:1001 /data

COPY --from=builder --chown=1001:1001 /app/build/libs/panopticum-all.jar app.jar

USER 1001

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD bash -ec 'exec 3<>/dev/tcp/127.0.0.1/8080; printf "GET /login HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n" >&3; IFS= read -r status <&3; case "$status" in HTTP/*\ 2*|HTTP/*\ 3*) exit 0 ;; *) exit 1 ;; esac'

ENTRYPOINT ["java", "-jar", "app.jar"]
