# Panopticum

A tool for developers and QA — web interface for viewing and managing database connections. Designed for deployment in Kubernetes.

## Stack

- **Backend:** Micronaut 4.x
- **Views:** Thymeleaf + HTMX
- **Auth:** HTTP Basic (login/password from environment)
- **Storage:** H2 (Flyway migrations)

## Features

- Authentication via login and password (Basic Auth)
- Add and store PostgreSQL connection settings
- Menu with list of connections and settings section
- View connected databases (MVP: stub, planned — collection browser and search)

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
