# Panopticum

A tool for developers and QA — web interface for viewing and managing database connections. Designed for deployment in Kubernetes or Docker.

## Stack

- **Backend:** Micronaut 4.x
- **Views:** Thymeleaf + HTMX
- **Auth:** HTTP Basic (login/password from environment)
- **Storage:** H2 (Flyway migrations)
- **i18n:** English and Russian

## Supported databases

| Type | Features |
|------|----------|
| **PostgreSQL** | Browse databases, schemas, tables; run SQL; edit rows |
| **MySQL (MariaDB)** | Browse databases and tables; run SQL; edit rows (if table has PK or unique index) |
| **MS SQL Server** | Browse databases, schemas, tables; run SQL; edit rows (if table has PK or unique index) |
| **Oracle Database** | Browse schemas and tables; run SQL; edit rows by ROWID |
| **MongoDB** | Browse databases and collections; run queries |
| **Redis** | Browse databases and keys; view key types and values |
| **ClickHouse** | Browse databases and tables; run SQL |
| **Cassandra** | Browse keyspaces and tables; run CQL; edit rows (when table has primary key) |

Connections are stored in H2. In Settings you can add connections, test them, and delete them.

## Features

- HTTP Basic Auth (credentials from env)
- Light and dark theme (toggle in header and on login page)
- Sidebar with saved connections and quick access to Settings
- Add, test, and remove connections per database type
- Browse metadata (schemas, tables, collections, keys) with pagination
- Execute SQL (PostgreSQL, MySQL/MariaDB, MS SQL Server, Oracle, ClickHouse, Cassandra CQL) and queries (MongoDB)
- Edit and save rows in detail view (PostgreSQL by ctid, MySQL/MS SQL Server when table has PK/unique, Oracle by ROWID, MongoDB, Redis, Cassandra when table has primary key)
- HTMX for partial updates without full page reloads
- Localization: EN and RU (browser or path)

## Running

```bash
./gradlew run
```

Application: **http://localhost:8080**

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PANOPTICUM_USER` | Basic Auth login | `admin` |
| `PANOPTICUM_PASSWORD` | Basic Auth password | `admin` |
| `PANOPTICUM_DB_PATH` | H2 database file path | `./data/panopticum` |
| `PANOPTICUM_CONNECTIONS_JSON` | JSON array of connections to load on first start (see below) | — |

### Bootstrap connections (first start only)

If the `db_connections` table is **empty** at startup, the app reads the environment variable `PANOPTICUM_CONNECTIONS_JSON` and inserts the given connections into H2. If there is already at least one connection in the database, the variable is ignored.

Value: a JSON array of connection objects. Each object can be specified in one of two ways:

1. **Explicit fields:** `name`, `type`, `host`, `port`, `database`, `username`, `password`. Supported `type` values: `postgresql`, `mongodb`, `redis`, `clickhouse`, `mysql`, `sqlserver`, `oracle`, `cassandra`.
2. **JDBC URL:** `name` and `jdbcUrl` (or `url`). The URL is parsed to derive type, host, port, database, username, and password. Supported for PostgreSQL, MySQL, MS SQL Server, Oracle, and ClickHouse (e.g. `jdbc:postgresql://user:pass@host:5432/dbname`, `jdbc:sqlserver://host:1433;databaseName=db;user=sa;password=secret`, `jdbc:oracle:thin:@//host:1521/XEPDB1`).

Example:

```json
[
  {"name": "prod-pg", "jdbcUrl": "jdbc:postgresql://app:secret@pg.svc:5432/mydb"},
  {"name": "analytics", "jdbcUrl": "jdbc:clickhouse://ch.svc:8123/default"},
  {"name": "cache", "type": "redis", "host": "redis.svc", "port": 6379}
]
```

For Helm: put the JSON in a Secret and mount it as the env var `PANOPTICUM_CONNECTIONS_JSON` (e.g. `valueFrom.secretKeyRef`). Use a Secret rather than a ConfigMap if the JSON contains passwords.

## Build

```bash
./gradlew build
```

JAR: `build/libs/panopticum-0.1-all.jar`

## Docker

### From GitHub Container Registry (GHCR)

Images are published automatically on tag push (see [CI/CD](#cicd)). Pull and run:

```bash
docker pull ghcr.io/thesharque/panopticum:latest
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  ghcr.io/thesharque/panopticum:latest
```

Use a version tag for a fixed release, e.g. `ghcr.io/thesharque/panopticum:v4.1.0`. If the package is private, log in first: `echo $GITHUB_TOKEN | docker login ghcr.io -u YOUR_GITHUB_USER --password-stdin`.

### From Docker Hub

Image on [Docker Hub](https://hub.docker.com/r/sharque/panopticum): `sharque/panopticum`

```bash
docker pull sharque/panopticum:latest
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  sharque/panopticum:latest
```

### Build locally

```bash
docker build -t panopticum:latest .
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  panopticum:latest
```

Open **http://localhost:8080**. For Kubernetes, use the same env vars and mount a volume at `/data` for H2 persistence.

## CI/CD

Push a version tag (e.g. `v4.1.0`) to trigger a GitHub Actions workflow that builds the Docker image once and pushes it to:

- [GitHub Container Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry): `ghcr.io/<owner>/panopticum:<tag>`
- [Docker Hub](https://hub.docker.com/r/sharque/panopticum): `<DOCKERHUB_USERNAME>/panopticum:<tag>` (if Docker Hub is enabled via variable and secrets)

```bash
git tag v4.1.0
git push origin v4.1.0
```

**Docker Hub:** to also push to Docker Hub, set a repository variable `ENABLE_DOCKERHUB` = `true` (Settings → Secrets and variables → Actions → Variables) and add secrets `DOCKERHUB_USERNAME` (your Docker Hub login) and `DOCKERHUB_TOKEN` ([access token](https://hub.docker.com/settings/security)). If you don’t set these, the workflow still runs and pushes only to GHCR.

Images will be available as `ghcr.io/<your-org>/panopticum:v0.1` and `ghcr.io/<your-org>/panopticum:latest` (and on Docker Hub as `<DOCKERHUB_USERNAME>/panopticum:v0.1` / `:latest` when configured). To deploy the image (e.g. update a Kubernetes deployment or pull on a server), add a deploy job or a separate workflow using the built image tag.
