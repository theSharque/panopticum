# Panopticum Helm Chart

A Helm chart for deploying Panopticum — a web interface for viewing and managing database connections.

## Features

- **Web UI** for browsing and querying databases
- **Multiple database support**: PostgreSQL, MySQL, MongoDB, Redis, ClickHouse, Oracle, Cassandra, Kafka, RabbitMQ, Elasticsearch
- **MCP endpoint** for AI agent integration
- **Light/Dark themes**
- **Data Diff** for comparing records across environments

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+
- PV provisioner support in the underlying infrastructure

## Installation

### Add the repository

```bash
helm repo add panopticum https://thesharque.github.io/panopticum
helm repo update
```

### Install the chart

```bash
# Install with default configuration
helm install my-panopticum panopticum/panopticum

# Install with custom credentials
helm install my-panopticum panopticum/panopticum \
  --set config.user=admin \
  --set config.password=securepassword

# Install with pre-configured connections
helm install my-panopticum panopticum/panopticum \
  --set-file config.connectionsJson=connections.json
```

### Port-forward for local access

```bash
kubectl port-forward svc/my-panopticum 8080:8080
```

Then open http://localhost:8080

## Configuration

### Basic Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `config.user` | Basic auth username | `admin` |
| `config.password` | Basic auth password | `changeme` |
| `config.dbPath` | H2 database path | `/data/panopticum` |
| `config.adminLock` | Disable connection management | `false` |
| `config.readOnly` | Read-only mode | `false` |

### Database Connections

You can pre-configure database connections using `config.connectionsJson`:

```yaml
config:
  connectionsJson: |
    [
      {"name": "prod/postgres", "jdbcUrl": "jdbc:postgresql://user:pass@pg.example.com:5432/mydb"},
      {"name": "prod/redis", "type": "redis", "host": "redis.example.com", "port": 6379},
      {"name": "staging/mongo", "type": "mongodb", "host": "mongo.example.com", "port": 27017, "database": "mydb"}
    ]
```

### Persistence

By default, the chart creates a PersistentVolumeClaim for the H2 database:

```yaml
persistence:
  enabled: true
  size: 5Gi
  storageClass: "standard"
```

### Ingress

```yaml
ingress:
  enabled: true
  className: nginx
  hosts:
    - host: panopticum.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: panopticum-tls
      hosts:
        - panopticum.example.com
```

## Database Support

| Database | Browse | Query | Edit |
|----------|--------|-------|------|
| PostgreSQL | ✅ | ✅ | ✅ |
| MySQL / MariaDB | ✅ | ✅ | ✅ |
| MS SQL Server | ✅ | ✅ | ✅ |
| Oracle | ✅ | ✅ | ✅ |
| MongoDB | ✅ | ✅ | ✅ |
| Redis / Valkey / Dragonfly / KeyDB | ✅ | ✅ | ✅ |
| ClickHouse | ✅ | ✅ | ❌ |
| Cassandra / ScyllaDB | ✅ | ✅ | ✅ |
| RabbitMQ | ✅ | ❌ | ❌ |
| Kafka | ✅ | ❌ | ❌ |
| Elasticsearch / OpenSearch | ✅ | ✅ | ✅ |

## API & MCP

- **Swagger UI**: `/swagger-ui`
- **MCP Endpoint**: `/mcp` (for AI agents like Cursor, Claude Desktop)

## Uninstall

```bash
helm uninstall my-panopticum
```

**Note**: This will not delete the PersistentVolumeClaim. To remove all data:

```bash
kubectl delete pvc my-panopticum-data
```