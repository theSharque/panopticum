# Panopticum

Инструмент для разработчиков и QA — веб-интерфейс для просмотра и управления подключениями к базам данных. Предназначен для развёртывания в Kubernetes или Docker.

## Возможности

- **Подключение** к PostgreSQL, MySQL, MongoDB, Redis, ClickHouse, Kafka, Elasticsearch, API Kubernetes, S3/MinIO, Prometheus/VictoriaMetrics и [другим источникам](#поддерживаемые-бд)
- **Просмотр** схем, таблиц, ключей, топиков; **Kubernetes**: namespace, pod-ы, Deployment, StatefulSet, Service, Ingress, ConfigMap, Secret (раскрытие по запросу, с аудитом — значение не пишется в логи), события; **S3**: бакеты и префиксы; **Prometheus**: jobs и метрики
- **Запросы** — SQL, CQL, MQL, PromQL; просмотр содержимого S3-объектов (JSON/CSV/Parquet/hex)
- **Сравнение** — Data Diff для записей между Dev / Stage / Prod
- **Интеграция** — MCP endpoint для AI-агентов (Cursor, Claude Desktop) — включая новый инструмент `describe-entity` для схемного контекста
- **Офлайн** — все ресурсы встроены, CDN не требуется

Подключения хранятся в H2. Добавляйте, проверяйте и удаляйте их в Настройках. Используйте `/` в именах для папок (например `Prod/PG`, `Dev/Mongo`).

## Быстрая установка

### Docker

```bash
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  ghcr.io/thesharque/panopticum:latest
```

Откройте **http://localhost:8080**.

Образы: [GHCR](https://github.com/thesharque/panopticum/pkgs/container/panopticum) `ghcr.io/thesharque/panopticum:latest`, [Docker Hub](https://hub.docker.com/r/sharque/panopticum) `sharque/panopticum:latest`. Для фиксированной версии — тег, например `:v8.0.3`.

### Helm

```bash
helm repo add panopticum https://thesharque.github.io/panopticum
helm repo update
helm install my-panopticum panopticum/panopticum
```

Затем `kubectl port-forward svc/my-panopticum 8080:8080` и откройте http://localhost:8080.

Подробнее в [panopticum-helm-chart/README.md](panopticum-helm-chart/README.md) — credentials, connections, ingress, persistence.

## Конфигурация

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `PANOPTICUM_USER` | Логин Basic Auth | `admin` |
| `PANOPTICUM_PASSWORD` | Пароль Basic Auth | `admin` |
| `PANOPTICUM_DB_PATH` | Путь к H2 | `./data/panopticum` |
| `PANOPTICUM_CONNECTIONS_JSON` | JSON-массив подключений при первом старте | — |
| `ADMIN_LOCK` | Запрет добавления/удаления подключений | `false` |
| `READ_ONLY` | Запрет редактирования данных | `false` |

### Bootstrap-подключения

Если `db_connections` **пуста** при старте, приложение загружает подключения из `PANOPTICUM_CONNECTIONS_JSON` (JSON-массив). Пример:

```json
[
  {"name": "prod-pg", "jdbcUrl": "jdbc:postgresql://app:secret@pg.svc:5432/mydb"},
  {"name": "cache", "type": "redis", "host": "redis.svc", "port": 6379},
  {"name": "docs", "type": "mongodb", "host": "mongo.svc", "port": 27017, "database": "mydb"}
]
```

Для Helm: используйте Secret с `valueFrom.secretKeyRef`, если JSON содержит пароли.

## Поддерживаемые БД

| Тип | Возможности |
|-----|--------------|
| **PostgreSQL / CockroachDB / YugabyteDB** | Просмотр; SQL; редактирование строк |
| **MySQL / MariaDB** | Просмотр; SQL; редактирование (с PK/unique) |
| **MS SQL Server** | Просмотр; SQL; редактирование (с PK/unique) |
| **Oracle Database** | Просмотр; SQL; редактирование по ROWID |
| **MongoDB** | Просмотр коллекций; запросы |
| **Redis / Dragonfly / Valkey / KeyDB** | Просмотр ключей; просмотр/редактирование значений |
| **ClickHouse** | Просмотр; SQL |
| **Cassandra / ScyllaDB** | Просмотр; CQL; редактирование (с PK) |
| **RabbitMQ** | Просмотр очередей; peek сообщений |
| **Kafka** | Просмотр топиков; peek записей |
| **Elasticsearch / OpenSearch** | Просмотр индексов; Query DSL; редактирование по _id |
| **Kubernetes** | URL API-сервера + bearer-токен; namespace через запятую; просмотр pod-ов, Deployment, StatefulSet, Service, Ingress, ConfigMap, Secret; tail логов; describe pod (контейнеры, образы, ресурсы, пробы, условия, события); события namespace; раскрытие секрета по запросу с аудитом (содержимое не попадает в логи). Мягкая обработка "нет доступа" — 401/403/404 как алерт, без краша |
| **S3 / MinIO** | Endpoint + access/secret key; просмотр бакетов и префиксов; peek объектов (JSON, CSV, Parquet head, hex). Регион опционален |
| **Prometheus / VictoriaMetrics** | Instant и range PromQL; просмотр jobs и метрик. Auth: Basic или Bearer-токен |

## MCP (AI-агенты)

MCP-совместимый endpoint `POST /mcp` для Cursor, Claude Desktop и др. Та же HTTP Basic Auth, что и для веб-интерфейса.

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

Замените `YWRtaW46YWRtaW4=` на Base64 от `username:password` (`echo -n "admin:changeme" | base64`).

### Инструменты MCP

| Инструмент | Описание |
|------------|----------|
| `list-data-sources` | Список всех настроенных подключений |
| `list-catalogs` | Базы данных / бакеты / jobs / namespace |
| `list-namespaces` | Схемы (где применимо) |
| `list-entities` | Таблицы / коллекции / объекты / метрики / pod-ы |
| `query-data` | SQL, CQL, MQL, PromQL или peek S3-объекта |
| `get-record-detail` | Получить запись по PK или document ID |
| `describe-entity` | Полная схема: колонки, типы, PK/FK/индексы, кол-во строк — устраняет потребность в `SELECT *`. Поддерживает все источники данных |

Для **Kubernetes**: `list-catalogs` → namespaces; `list-entities` → pod-ы; `query-data` → tail логов.
Для **S3**: `list-catalogs` → бакеты; `list-entities` → объекты; `query-data` → peek содержимого.
Для **Prometheus**: `list-catalogs` → jobs; `list-entities` → имена метрик; `query-data` → instant PromQL.

## Стек

- Micronaut 4.x, Thymeleaf, HTMX
- REST API + Swagger UI (`/swagger-ui`)
- H2 + Flyway
- i18n: EN / RU

## Сборка

```bash
./gradlew build
```

JAR: `build/libs/panopticum-all.jar`

## CI/CD

Пуш тега версии (например `v8.0.3`) запускает GitHub Actions — сборка и пуш Docker-образов в GHCR и Docker Hub.
