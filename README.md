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
| **PostgreSQL** | Browse databases, schemas, tables; run SQL |
| **MongoDB** | Browse databases and collections; run queries |
| **Redis** | Browse databases and keys; view key types and values |
| **ClickHouse** | Browse databases and tables; run SQL |

Connections are stored in H2. In Settings you can add connections, test them, and delete them.

## Features

- HTTP Basic Auth (credentials from env)
- Sidebar with saved connections and quick access to Settings
- Add, test, and remove connections per database type
- Browse metadata (schemas, tables, collections, keys) with pagination
- Execute SQL (PostgreSQL, ClickHouse) and queries (MongoDB)
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

## Build

```bash
./gradlew build
```

JAR: `build/libs/panopticum-0.1-all.jar`

## Docker

Build image:

```bash
docker build -t panopticum:latest .
```

Run (persist H2 data and set auth via env):

```bash
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  panopticum:latest
```

Open **http://localhost:8080**. For Kubernetes, use the same env vars and mount a volume at `/data` for H2 persistence.

## CI/CD

Push a version tag (e.g. `v0.1`, `v1.0.0`) to trigger a GitHub Actions workflow that:

- Builds the Docker image
- Pushes it to [GitHub Container Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry) as `ghcr.io/<owner>/<repo>:<tag>`

```bash
git tag v0.1
git push origin v0.1
```

Image will be available as `ghcr.io/<your-org>/panopticum:v0.1` and `ghcr.io/<your-org>/panopticum:latest`. To deploy the image (e.g. update a Kubernetes deployment or pull on a server), add a deploy job to `.github/workflows/docker-build-push.yml` or a separate workflow that runs after this one, using the built image digest or tag.
