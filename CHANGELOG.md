# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
