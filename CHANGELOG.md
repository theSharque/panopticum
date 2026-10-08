# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [8.4.9] - 2026-10-08

### Security

- lz4-java alignment to at.yawk.lz4 1.11.4 (CVE-2026-106449, CVE-2026-106450, CVE-2026-106451, CVE-2026-106452, CVE-2026-106453); keep Kafka/ClickHouse on the same coordinate

## [8.4.8] - 2026-10-07

### Security

- Jackson alignment to 2.21.7 (CVE-2026-91777, CVE-2026-91776, CVE-2026-68497, CVE-2026-83557, CVE-2026-19032 on jackson-databind; CVE-2026-89407, CVE-2026-89425 on jackson-core)
- Docker runtime: `apk upgrade` `zlib` to 1.3.2-r1 (CVE-2026-85091)

## [8.4.7] - 2026-09-11

### Fixed

- MCP `tools/call` tool-level failures return `isError: true` with the error text instead of JSON-RPC `-32603 Internal error`
- Kubernetes `list-entities`: 401/403/404 on pods are returned as errors instead of an empty pod list
- Kubernetes `describe-entity`: pod access failures (not found, forbidden) surface the Kubernetes error instead of a generic failure

## [8.4.6] - 2026-08-27

### Security

- Docker runtime: `apk upgrade` `libssl3` / `libcrypto3` to 3.5.8 (CVE-2026-18798, CVE-2026-14457, CVE-2026-63072, CVE-2026-14456, CVE-2026-63075, CVE-2026-54874, CVE-2026-63076, CVE-2026-63074, CVE-2026-75803); OpenSSL CLI already removed. Fresh Temurin Alpine also brings `busybox` 1.37.0-r31 (CVE-2025-60876 was `<=1.37.0-r30`)

## [8.4.5] - 2026-08-20

### Security

- HttpComponents alignment: `httpclient5` 5.6.3 (CVE-2026-64607), `httpcore5` / `httpcore5-h2` 5.4.3 (CVE-2026-54399, CVE-2026-54428); ClickHouse JDBC otherwise keeps 5.3.4/5.4.4 and Micronaut BOM constrains `httpclient5` to 5.5

## [8.4.4] - 2026-07-27

### Security

- Jetty alignment to 12.1.10 (CVE-2026-10050, CVE-2026-10051, CVE-2026-6790, CVE-2026-8384)
- lz4-java alignment to at.yawk.lz4 1.11.1 (CVE-2026-59949); unify Kafka and ClickHouse on the same coordinate
- ClickHouse JDBC 0.9.8 without `:all` classifier so Scout no longer reports shaded lz4 1.10.x

## [8.4.3] - 2026-07-20

### Fixed

- CodeMirror JSON/SQL editors: Ctrl+F opens an in-app search panel fixed to the viewport
- H2 metadata store: enable `AUTO_SERVER=TRUE` (port 9099) to reduce file-lock failures

## [8.4.2] - 2026-07-13

### Security

- Jackson alignment to 2.21.5 (CVE-2026-54515 on jackson-databind)
- Logback alignment to 1.5.35 (CVE-2026-10532, CVE-2026-9828)

## [8.4.1] - 2026-06-28

### Security

- Jackson alignment to 2.21.4 (CVE fixes for jackson-core and jackson-databind)
- Docker runtime: remove unused `p11-kit` / `p11-kit-trust` after `apk upgrade`

## [8.4.0] - 2026-06-18

### Added

- Activity audit: structured `AUDIT` lines to stdout for browse (`OPEN_DATABASE` / `OPEN_SCHEMA` / `OPEN_TABLE`), queries (`RUN_QUERY` with `kind` only — no SQL text), row updates, connection CRUD, API and MCP calls; toggle via `AUDIT_ENABLED` / `panopticum.audit.enabled`

### Fixed

- SQL/CQL query result partials: column sort forms no longer break Thymeleaf rendering (HTTP 500 after a successful `SELECT` in the query editor)

## [8.3.0] - 2026-06-18

### Fixed

- SQL table browse no longer wraps queries with default `ORDER BY 1` on ClickHouse, PostgreSQL, MySQL, SQL Server, and Oracle; pagination uses `SqlPagingSupport` with dialect-specific lightweight LIMIT/TOP/ROWNUM

### Changed

- MCP `resolve-panopticum-link` resolves breadcrumb copy paths (connection names with slashes) via shared `BreadcrumbPathHelper`

## [8.2.14] - 2026-06-12

### Changed

- Controller layer: shared `AbstractConnectionApiController` / `AbstractConnectionUiController`, `QueryResultModelHelper`, `AdminLockGuard`, `ConnectionTestHelper`; unified API/UI error constants (`ApiErrors`, `ErrorKeys`)
- Service layer: `ServiceQueryErrors`, `QueryResultMapper`; JDBC metadata services use shared error keys and query result mapping (~200 lines removed)

## [8.2.13] - 2026-06-12

### Changed

- Docker runtime: `apk update` + upgrade (OpenSSL 3.5.7); remove unused `openssl` CLI and `coreutils`; HEALTHCHECK sends HTTP GET to `/actuator/health/liveness` via `nc` (no `wget`)

## [8.2.12] - 2026-06-05

### Fixed

- MCP `get-record-detail` for Redis: read key value via `entity` (key name) and `catalog` (db index)

## [8.2.11] - 2026-06-04

### Added

- Micronaut Management: anonymous `/actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness` (Helm probes and Docker HEALTHCHECK)

### Changed

- Docker runtime (`eclipse-temurin:17-jre-alpine`): `apk upgrade` and remove Temurin-bundled packages not needed at runtime (`gnupg`, `fontconfig`, `ttf-dejavu` and transitive libs); drop `bash`, Docker healthcheck uses `wget` on actuator liveness
- Release CI: `docker/build-push-action` builds with `squash: true` so scanners see the flattened image after package cleanup

## [8.2.10] - 2026-06-04

### Added

- MCP tool `resolve-panopticum-link`: parse Panopticum UI URL or breadcrumb path into `connectionId` and MCP scope (`catalog`, `namespace`, `entity`)
- On resolve failure, `availablePaths` lists configured UI paths from H2 only (`DbConnectionService.listConfiguredUiPaths()` via `DbConnectionRepository` and `ConnectionType.uiPathPrefix`)

### Changed

- MCP `tools/call` errors with tool `content` return JSON-RPC result (`isError: true`) instead of dropping the payload

## [8.2.9] - 2026-05-29

### Changed

- Architecture consistency: `ConnectionType` registry in `core.model` centralizes type ids, ports, UI/API paths, query formats, and hierarchy models
- Shared entity models (`EntityDescription`, `ColumnInfo`, `IndexInfo`, `ForeignKeyInfo`) moved from `mcp.model` to `core.model`
- App config classes consolidated under `com.panopticum.core.config`
- Browser services renamed to `*MetadataService` (Couchbase, Elasticsearch, RabbitMQ)
- PostgreSQL UI/API paths: `/pg/*` → `/postgres/*`; classes `Pg*` → `Postgres*`
- SQL Server package and paths: `mssql` → `sqlserver`; classes `Mssql*` → `SqlServer*`
- S3 and Prometheus connection tests routed through unified `ConnectionTestService` with HTTPS override
- RabbitMQ MCP `query-data`: optional `publish` argument replaces JSON-in-query hack (legacy path kept with deprecation log)

### Added

- Flyway `V003`: normalize legacy `db_connections.type = 'mssql'` to `sqlserver`

## [8.2.8] - 2026-05-28

### Fixed

- ClickHouse, MySQL, MSSQL, and Oracle JDBC connections on Java 17 — Derby downgraded from `10.17.1.0` (Java 21+) to `10.16.1.1`, which had broken `ServiceLoader` driver registration in the fat JAR
- ClickHouse driver explicitly registered before `DriverManager` use (same pattern as PostgreSQL)

### Added

- Spotless Gradle plugin: removes unused imports and trailing whitespace; runs automatically before Java compilation

## [8.0.3] - 2026-05-04

### Security

- Direct dependencies for SBOM / vulnerability scanners: `org.apache.zookeeper:zookeeper:3.8.6`, `io.airlift:aircompressor:2.0.3`, `org.eclipse.jetty:jetty-server` / `jetty-http` `12.1.8` (alongside existing resolution pins)
- `resolutionStrategy` notes: CVE-2025-11143 on Jetty HTTP (`>=12.1.5`), ZooKeeper CVE-2026-24308 / CVE-2026-24281 advisory range text, MSSQL JDBC CVE-2025-59250 clarification for `13.4.0.jre11`

## [8.0.2] - 2026-05-03

### Security

- ZooKeeper `3.8.6` via Gradle resolution pin (addresses CVE-2026-24308 / CVE-2026-24281 for the `3.8.4` line pulled by Hadoop)
- Aircompressor `2.0.3` (CVE-2025-67721)
- Jetty 12.x core stack `12.1.8`: `jetty-server`, `jetty-http`, `jetty-io`, `jetty-util`, `jetty-security`, `jetty-xml`, `jetty-util-ajax` (CVE-2026-1605, CVE-2026-2332); Hadoop still bundles Jetty `9.4.x` servlet/webapp separately
- MSSQL JDBC `13.4.0.jre11` (driver refresh; avoids Docker Scout flagging `12.10.2` vs `12.10.2.jre11` artifact naming)

## [8.0.1] - 2026-05-02

### Security

- Dependency upgrades addressing CVEs in Parquet/Avro/Hadoop/ZooKeeper/Kerby/aircompressor chain (`parquet-avro` 1.15.2, `hadoop-common` 3.4.1), pinned `commons-beanutils` 1.11.0
- Docker images based on `eclipse-temurin` Ubuntu Noble (`17-*-noble`); CI Docker build uses `pull: true` for fresh base layers
- Helm packaging workflow uses Helm `v3.20.2`

## [8.0.0] - 2026-05-01

### Added

- **`describe-entity` MCP tool** — returns full schema context for any data source (columns, types, PK/FK/indexes, approximate row count). AI agents can now skip `SELECT *` and get precise schema context, reducing token usage.
  - PostgreSQL, MySQL, MSSQL, Oracle — via `information_schema` + system tables
  - ClickHouse — `system.columns` / `system.tables`
  - Cassandra — `system_schema.columns`
  - MongoDB — schema inferred from sampled documents (`sampleSize` parameter, default 100)
  - Elasticsearch — index mappings
  - Redis — key type, TTL, encoding
  - Kafka — partition info
  - RabbitMQ — queue stats (messages, consumers, vhost)
- **Kubernetes read-only expansion**
  - `describe pod` — detailed view: containers, images, resources, probes, conditions, events
  - Namespace events listing
  - Read-only listings: Deployments, StatefulSets, Services, Ingresses, ConfigMaps, Secrets
  - Secret value reveal (unmask on demand) with audit log (value never logged, only metadata)
  - Graceful "no access" handling via `AccessResult` — 401/403/404 shown as soft UI alert, no 5xx
- **S3 / MinIO integration** — new connection type `s3`
  - Browse buckets and object prefixes (folder-style navigation)
  - Peek object contents: JSON, CSV, text, Parquet head (schema + first rows), binary as hex dump
  - MCP: `list-catalogs` → buckets, `list-entities` → objects/prefixes, `query-data` → peek, `describe-entity` → object metadata
- **Prometheus / VictoriaMetrics integration** — new connection type `prometheus`
  - Instant PromQL query and range queries (start/end/step)
  - Browse jobs and metrics list
  - MCP: `list-catalogs` → jobs, `list-entities` → metric names, `query-data` → instant PromQL, `describe-entity` → metric labels
  - Auth: Basic Auth (username + password) or Bearer token (empty username, token in password)
- `AccessResult<T>` envelope for unified soft-error handling across Kubernetes, S3, Prometheus
- i18n keys for new features: `MessagesS3`, `MessagesPrometheus`, extended `MessagesKubernetes`

## [7.3.0] - 2026-03-12

### Added

- MCP service

## [7.2.0] - 2026-03-11

### Added

- Syntax highlighting to query editors
- Light theme support to CodeMirror editors
- Custom light theme styling for CodeMirror editors

### Changed

- Remove margins, padding, and border from query-panel
- Remove h3 query panel titles to reduce vertical clutter
- Remove redundant h1 headings from query pages

### Fixed

- Light theme add SQL

## [7.1.1] - 2026-03-11

### Added

- Store tree state in browser

## [7.1.0] - 2026-03-11

### Added

- Connection tree structure
- Swagger redirect

### Fixed

- Readmes
- CHANGELOG

## [7.0.0] - 2026-03-08

### Added

- REST API for all operations (connections, databases, SQL/query execution, row edit, etc.)
- Swagger/OpenAPI 3.0 with interactive Swagger UI at `/swagger-ui`

### Fixed

- Build configuration

## [6.5.1]

### Added

- DB scroll

## [6.5.0]

### Added

- DB row edit feature

## [6.4.0]

### Added

- Simple diff view

## [6.3.1]

### Fixed

- MongoDB update

## [6.3.0]

### Added

- JSON syntax highlighting and offline support

## [6.2.0]

### Added

- TextArea autosize

## [6.1.0]

### Added

- SQL/Query history

## [6.0.6]

### Added

- Version tag display

## [6.0.5]

### Changed

- Request size limit (pick up large request body)

## [6.0.4]

### Fixed

- Delete operation, MongoDB ID handling

## [6.0.3]

### Added

- All supported DB types in connections

## [6.0.2]

### Fixed

- Favicon path

## [6.0.1]

### Added

- Application icon

## [6.0.0]

### Added

- Elasticsearch support

## [5.6.0]

### Added

- READ_ONLY mode

## [5.5.1]

### Fixed

- Docker configuration

## [5.5.0]

### Fixed

- Application name display

## [5.2.0]

### Fixed

- Details view

## [5.1.0]

### Added

- Simple SQL search

## [5.0.1]

### Added

- Redis search

## [5.0.0]

### Added

- RabbitMQ support

## [4.5.1]

### Added

- Odd/even row highlighting (zebra striping)

## [4.5.0]

### Fixed

- Various fixes

## [4.3.0]

### Added

- MS SQL Server support

## [4.2.0]

### Added

- Default connections on startup

## [4.1.0]

### Added

- Cassandra support

## [4.0.0]

### Added

- MySQL support

## [3.0.1]

### Fixed

- Security issues

## [3.0.0]

### Fixed

- Header click behavior

## [2.0.0]

### Added

- Two themes (light/dark)
- New UI style

## [0.1]

### Added

- CI/CD pipeline
- Initial Panopticum MVP (Micronaut + Thymeleaf + HTMX)
