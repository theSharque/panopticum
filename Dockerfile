FROM eclipse-temurin:17-jdk AS builder

WORKDIR /app

COPY gradle gradle
COPY gradlew build.gradle settings.gradle gradle.properties ./
COPY src src/

RUN ./gradlew shadowJar --no-daemon

FROM eclipse-temurin:17-jre

ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION

RUN groupadd -g 1001 app && useradd -u 1001 -g app -s /bin/false app

WORKDIR /app

ENV PANOPTICUM_DB_PATH=/data/panopticum

RUN mkdir -p /data && chown -R 1001:1001 /data

COPY --from=builder --chown=1001:1001 /app/build/libs/panopticum-all.jar app.jar

USER 1001

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/login || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
