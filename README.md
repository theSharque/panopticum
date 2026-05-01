# Panopticum

A tool for developers and QA — web interface for viewing and managing database connections. Designed for deployment in Kubernetes or Docker.

## What it does

- **Connect** to PostgreSQL, MySQL, MongoDB, Redis, ClickHouse, Kafka, Elasticsearch, Kubernetes API, S3/MinIO, Prometheus/VictoriaMetrics, and [many more](#supported-databases)
- **Browse** schemas, tables, keys, topics — with pagination and tree view; **Kubernetes**: namespaces, pods, deployments, services, secrets (with reveal), events; **S3**: buckets and object prefixes; **Prometheus**: jobs and metrics
- **Query** — run SQL, CQL, MQL, PromQL; peek S3 object contents (JSON/CSV/Parquet/hex)
- **Compare** — Data Diff for records across Dev / Stage / Prod
- **Integrate** — MCP endpoint for AI agents (Cursor, Claude Desktop) — including new `describe-entity` tool for schema context
- **Offline** — all assets bundled, no CDN

Connections are stored in H2. Add, test, and remove them in Settings. Use `/` in connection names for folders (e.g. `Prod/PG`, `Dev/Mongo`).

## Quick install

### Docker

```bash
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  ghcr.io/thesharque/panopticum:latest
```

Open **http://localhost:8080**.

Images: [GHCR](https://github.com/thesharque/panopticum/pkgs/container/panopticum) `ghcr.io/thesharque/panopticum:latest`, [Docker Hub](https://hub.docker.com/r/sharque/panopticum) `sharque/panopticum:latest`. For a fixed version use a tag, e.g. `:v4.1.0`.

### Helm

```bash
helm repo add panopticum https://thesharque.github.io/panopticum
helm repo update
helm install my-panopticum panopticum/panopticum
```

Then `kubectl port-forward svc/my-panopticum 8080:8080` and open http://localhost:8080.

See [panopticum-helm-chart/README.md](panopticum-helm-chart/README.md) for configuration (credentials, connections, ingress, persistence).

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PANOPTICUM_USER` | Basic Auth login | `admin` |
| `PANOPTICUM_PASSWORD` | Basic Auth password | `admin` |
| `PANOPTICUM_DB_PATH` | H2 database path | `./data/panopticum` |
| `PANOPTICUM_CONNECTIONS_JSON` | JSON array of connections to load on first start | — |
| `ADMIN_LOCK` | Disable adding/removing connections | `false` |
| `READ_ONLY` | Disable data editing | `false` |

### Bootstrap connections

If `db_connections` is **empty** at startup, the app loads connections from `PANOPTICUM_CONNECTIONS_JSON` (JSON array). Example:

```json
[
  {"name": "prod-pg", "jdbcUrl": "jdbc:postgresql://app:secret@pg.svc:5432/mydb"},
  {"name": "cache", "type": "redis", "host": "redis.svc", "port": 6379},
  {"name": "docs", "type": "mongodb", "host": "mongo.svc", "port": 27017, "database": "mydb"}
]
```

For Helm: use a Secret with `valueFrom.secretKeyRef` if the JSON contains passwords.

## Supported databases

| Type | Features |
|------|----------|
| **PostgreSQL / CockroachDB / YugabyteDB** | Browse; SQL; edit rows |
| **MySQL / MariaDB** | Browse; SQL; edit (with PK/unique) |
| **MS SQL Server** | Browse; SQL; edit (with PK/unique) |
| **Oracle Database** | Browse; SQL; edit by ROWID |
| **MongoDB** | Browse collections; queries |
| **Redis / Dragonfly / Valkey / KeyDB** | Browse keys; view/edit values |
| **ClickHouse** | Browse; SQL |
| **Cassandra / ScyllaDB** | Browse; CQL; edit (with PK) |
| **RabbitMQ** | Browse queues; peek messages |
| **Kafka** | Browse topics; peek records |
| **Elasticsearch / OpenSearch** | Browse indices; Query DSL; edit by _id |
| **Kubernetes** | API server URL + bearer token; namespaces (comma-separated); browse pods, deployments, services, ingresses, configmaps, secrets; tail logs; describe pod; namespace events. Graceful "no access" — 401/403 shown as soft alert |
| **S3 / MinIO** | Endpoint + access/secret key; browse buckets and prefixes; peek objects (JSON, CSV, Parquet head, hex). Region optional |
| **Prometheus / VictoriaMetrics** | Instant and range PromQL; browse jobs and metrics. Auth: Basic or Bearer token |

## MCP (AI agents)

MCP-compatible endpoint at `POST /mcp` for Cursor, Claude Desktop, etc. Same HTTP Basic Auth as the UI.

**Cursor** (`.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "panopticum": {
      "url": "http://localhost:8080/mcp",
      "headers": { "Authorization": "Basic YWRtaW46YWRtaW4=" }
    }
  }
}
```

Replace `YWRtaW46YWRtaW4=` with Base64 of `username:password` (`echo -n "admin:changeme" | base64`).

### MCP Tools

| Tool | Description |
|------|-------------|
| `list-data-sources` | List all configured connections |
| `list-catalogs` | List databases / buckets / jobs / namespaces |
| `list-namespaces` | List schemas (where applicable) |
| `list-entities` | List tables / collections / objects / metrics / pods |
| `query-data` | Execute SQL, CQL, MQL, PromQL or peek S3 objects |
| `get-record-detail` | Fetch a single record by PK or document ID |
| `describe-entity` | Full schema: columns, types, PK/FK/indexes, row count — eliminates `SELECT *`. Supports all data sources |

For **Kubernetes**: `list-catalogs` → namespaces; `list-entities` → pods; `query-data` → tail logs.
For **S3**: `list-catalogs` → buckets; `list-entities` → objects; `query-data` → peek content.
For **Prometheus**: `list-catalogs` → jobs; `list-entities` → metric names; `query-data` → instant PromQL.

## Stack

- Micronaut 4.x, Thymeleaf, HTMX
- REST API + Swagger UI (`/swagger-ui`)
- H2 + Flyway
- EN/RU i18n

## Build

```bash
./gradlew build
```

JAR: `build/libs/panopticum-all.jar`

## CI/CD

Push a version tag (e.g. `v4.1.0`) to trigger a GitHub Actions workflow that builds and pushes Docker images to GHCR and Docker Hub.
