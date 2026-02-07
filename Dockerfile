FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

RUN apk add --no-cache bash

COPY gradle gradle
COPY gradlew build.gradle settings.gradle gradle.properties ./

RUN ./gradlew shadowJar --no-daemon

FROM eclipse-temurin:17-jre-alpine

RUN apk add --no-cache wget \
    && addgroup -g 1000 app && adduser -u 1000 -G app -D app

WORKDIR /app

ENV PANOPTICUM_DB_PATH=/data/panopticum

RUN mkdir -p /data && chown -R app:app /data

COPY --from=builder --chown=app:app /app/build/libs/panopticum-*-all.jar app.jar

USER app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/login || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
