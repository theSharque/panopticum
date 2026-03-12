# Panopticum

Инструмент для разработчиков и QA — веб-интерфейс для просмотра и управления подключениями к базам данных. Предназначен для развёртывания в Kubernetes или Docker.

## Стек

- **Backend:** Micronaut 4.x
- **Views:** Thymeleaf + HTMX
- **API:** REST API для всех операций; Swagger/OpenAPI 3.0
- **Auth:** HTTP Basic (логин/пароль из environment)
- **Storage:** H2 (миграции Flyway)
- **i18n:** английский и русский

## Поддерживаемые БД

| Тип | Возможности |
|-----|--------------|
| **PostgreSQL / CockroachDB / YugabyteDB** | Просмотр баз, схем, таблиц; выполнение SQL; редактирование строк |
| **MySQL / MariaDB** | Просмотр баз и таблиц; выполнение SQL; редактирование строк (если у таблицы есть PK или unique-индекс) |
| **MS SQL Server** | Просмотр баз, схем, таблиц; выполнение SQL; редактирование строк (если у таблицы есть PK или unique-индекс) |
| **Oracle Database** | Просмотр схем и таблиц; выполнение SQL; редактирование строк по ROWID |
| **MongoDB** | Просмотр баз и коллекций; выполнение запросов |
| **Redis / Dragonfly / Valkey / KeyDB** | Просмотр баз и ключей; типы и значения |
| **ClickHouse** | Просмотр баз и таблиц; выполнение SQL |
| **Cassandra / ScyllaDB** | Просмотр keyspace и таблиц; выполнение CQL; редактирование строк (если у таблицы есть primary key) |
| **RabbitMQ** | Просмотр очередей; просмотр сообщений (peek, только чтение, без редактирования) |
| **Kafka** | Просмотр топиков и партиций; просмотр записей (только чтение) |
| **Elasticsearch / OpenSearch** | Просмотр индексов; поиск (Query DSL); просмотр и редактирование документов по _id (без удаления) |

Подключения хранятся в H2. В настройках можно добавлять подключения, проверять их и удалять. Имена подключений могут содержать `/` для группировки в папки в боковой панели (например `Prod/PG`, `Prod/Mongo`). Имена должны быть уникальны и не могут заканчиваться на `/`.

## Возможности

- HTTP Basic Auth (учётные данные из env)
- Светлая и тёмная тема (переключатель в шапке и на странице входа)
- Боковая панель со списком сохранённых подключений и быстрым доступом в Настройки; древовидное представление — используйте `/` в имени подключения для создания папок (например `Dev/PG`, `Dev/Back/Oracle`), папки сворачиваются, состояние сохраняется в рамках сессии
- Добавление, проверка и удаление подключений для каждого типа БД
- Просмотр метаданных (схемы, таблицы, коллекции, ключи) с постраничной навигацией
- Выполнение SQL (PostgreSQL / CockroachDB / YugabyteDB, MySQL / MariaDB, MS SQL Server, Oracle, ClickHouse, Cassandra / ScyllaDB CQL) и запросов (MongoDB)
- Редактирование и сохранение строк в детальном просмотре (PostgreSQL по ctid, MySQL/MS SQL Server при наличии PK/unique, Oracle по ROWID, MongoDB, Redis / Dragonfly / Valkey / KeyDB, Cassandra / ScyllaDB при наличии primary key, Elasticsearch / OpenSearch — документ по _id)
- REST API для всех операций с БД (подключения, просмотр, SQL/запросы, редактирование строк и т.д.)
- Swagger UI по адресу `/swagger-ui` для интерактивной документации API (OpenAPI 3.0); `/swagger` и `/swagger/index.html` делают редирект туда
- HTMX для частичного обновления без перезагрузки страницы
- Подсветка синтаксиса JSON (read-only блоки и редактор CodeMirror на страницах detail)
- Офлайн / закрытый контур: все внешние ресурсы (HTMX, Prism, CodeMirror, шрифты) включены локально — CDN не требуется
- Локализация: EN и RU (по браузеру или пути)
- **Data Diff (сравнение данных):** добавление записей с любой страницы detail в список сравнения (хранится в localStorage браузера); визуальное сравнение side-by-side между окружениями (Dev vs Stage vs Prod) или разными типами БД
- **MCP (Model Context Protocol):** JSON-RPC endpoint по адресу `/mcp` для AI-агентов — список источников данных, просмотр каталогов/схем/таблиц, выполнение запросов и сравнение записей между БД (Postgres vs Mongo и т.д.)

## MCP (Model Context Protocol)

Panopticum предоставляет MCP-совместимый JSON-RPC endpoint по адресу `/mcp` для AI-агентов (Cursor, Claude Desktop и др.). Агенты могут получать список подключений, просматривать метаданные, выполнять запросы и сравнивать данные между разными типами БД. Все MCP-запросы требуют HTTP Basic Auth (те же учётные данные, что и для веб-интерфейса).

**Endpoint:** `POST /mcp`  
**Аутентификация:** HTTP Basic (те же `PANOPTICUM_USER` / `PANOPTICUM_PASSWORD`)

**Tools:**

| Tool | Описание |
|------|----------|
| `list-data-sources` | Безопасный список подключений (id, name, dbType, queryFormat, hierarchyModel — без кредов) |
| `list-catalogs` | Базы данных / keyspaces / топики (Kafka) / db 0–15 (Redis) / индексы (Elasticsearch) |
| `list-namespaces` | Схемы (Postgres, MSSQL, Oracle) |
| `list-entities` | Таблицы / коллекции / партиции (Kafka, catalog=топик) |
| `query-data` | Выполнение SQL/CQL/MQL/JSON; возвращает унифицированный envelope (макс. 100 строк) |
| `get-record-detail` | Одна запись/документ для сравнения |

**Проверка через curl:**

```bash
# Инициализация
curl -u admin:admin -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'

# Список tools
curl -u admin:admin -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'

# Список источников данных
curl -u admin:admin -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list-data-sources","arguments":{}}}'

# Список каталогов (Kafka: топики, Redis: db 0-15, Elasticsearch: индексы)
curl -u admin:admin -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list-catalogs","arguments":{"connectionId":481}}}'

# Запрос Kafka (catalog=топик, query=JSON с partition, fromOffset, count, fromEnd)
curl -u admin:admin -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"query-data","arguments":{"connectionId":481,"catalog":"my-topic","query":"{\"partition\":0,\"fromEnd\":true,\"count\":10}"}}}'

# Запрос Redis (catalog=dbIndex, query=glob-паттерн)
curl -u admin:admin -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"query-data","arguments":{"connectionId":353,"catalog":"0","query":"user:*"}}}'

# Запрос Elasticsearch (catalog/entity=индекс, query=JSON DSL)
curl -u admin:admin -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"query-data","arguments":{"connectionId":577,"entity":"my-index","query":"{\"query\":{\"match_all\":{}}}"}}}'
```

**Cursor** (`.cursor/mcp.json` или `~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "panopticum": {
      "url": "http://localhost:8080/mcp",
      "headers": {
        "Authorization": "Basic YWRtaW46YWRtaW4="
      }
    }
  }
}
```

Замените `YWRtaW46YWRtaW4=` на Base64 от `username:password` (например `echo -n "admin:changeme" | base64`).

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json` на macOS):

```json
{
  "mcpServers": {
    "panopticum": {
      "url": "http://localhost:8080/mcp",
      "headers": {
        "Authorization": "Basic YWRtaW46YWRtaW4="
      }
    }
  }
}
```

**Windsurf и другие MCP-клиенты:** используйте тот же формат `url` + `headers`. Убедитесь, что клиент поддерживает HTTP POST JSON-RPC (а не только stdio или SSE).

## Запуск

```bash
./gradlew run
```

Приложение: **http://localhost:8080**

Swagger UI: **http://localhost:8080/swagger-ui** (или http://localhost:8080/swagger, http://localhost:8080/swagger/index.html)

## Конфигурация

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `PANOPTICUM_USER` | Логин для Basic Auth | `admin` |
| `PANOPTICUM_PASSWORD` | Пароль для Basic Auth | `admin` |
| `PANOPTICUM_DB_PATH` | Путь к файлам H2 | `./data/panopticum` |
| `PANOPTICUM_CONNECTIONS_JSON` | JSON-массив подключений для загрузки при первом старте (см. ниже) | — |
| `ADMIN_LOCK` | При `true` запрещает добавление и удаление подключений (UI и API) | `false` |
| `READ_ONLY` | При `true` запрещает изменение данных на страницах detail и через API (обновление строк, сохранение ключей Redis) | `false` |

### Подключения при первом старте (bootstrap)

Если при старте приложения таблица `db_connections` **пуста**, приложение читает переменную окружения `PANOPTICUM_CONNECTIONS_JSON` и добавляет указанные подключения в H2. Если в базе уже есть хотя бы одно подключение, переменная не используется.

Значение — JSON-массив объектов подключений. Каждый объект можно задать одним из двух способов:

1. **Явные поля:** `name`, `type`, `host`, `port`, `database`, `username`, `password`. Поддерживаемые значения `type`: `postgresql`, `mongodb`, `redis`, `clickhouse`, `mysql`, `sqlserver`, `oracle`, `cassandra`, `rabbitmq`, `kafka`, `elasticsearch`.
2. **JDBC-строка:** поля `name` и `jdbcUrl` (или `url`). По URL извлекаются тип, хост, порт, база, пользователь и пароль. Поддерживается для PostgreSQL, MySQL, MS SQL Server, Oracle и ClickHouse (например `jdbc:postgresql://user:pass@host:5432/dbname`, `jdbc:sqlserver://host:1433;databaseName=db;user=sa;password=secret`, `jdbc:oracle:thin:@//host:1521/XEPDB1`).

Пример:

```json
[
  {"name": "prod-pg", "jdbcUrl": "jdbc:postgresql://app:secret@pg.svc:5432/mydb"},
  {"name": "prod-mysql", "jdbcUrl": "jdbc:mysql://app:secret@mysql.svc:3306/mydb"},
  {"name": "prod-mssql", "jdbcUrl": "jdbc:sqlserver://mssql.svc:1433;databaseName=mydb;user=sa;password=secret"},
  {"name": "prod-oracle", "jdbcUrl": "jdbc:oracle:thin:app/secret@//oracle.svc:1521/XEPDB1"},
  {"name": "analytics", "jdbcUrl": "jdbc:clickhouse://clickhouse.svc:8123/default"},
  {"name": "docs", "type": "mongodb", "host": "mongo.svc", "port": 27017, "database": "mydb", "username": "app", "password": "secret"},
  {"name": "cache", "type": "redis", "host": "redis.svc", "port": 6379},
  {"name": "events-db", "type": "cassandra", "host": "cassandra.svc", "port": 9042, "database": "mykeyspace", "username": "cassandra", "password": "secret"},
  {"name": "broker", "type": "rabbitmq", "host": "rabbitmq.svc", "port": 15672, "username": "guest", "password": "guest"},
  {"name": "streaming", "type": "kafka", "host": "kafka.svc", "port": 9092},
  {"name": "search", "type": "elasticsearch", "host": "es.svc", "port": 9200, "username": "elastic", "password": "secret"}
]
```

Для Helm: поместите JSON в Secret и смонтируйте его в переменную окружения `PANOPTICUM_CONNECTIONS_JSON` (например через `valueFrom.secretKeyRef`). Для JSON с паролями используйте Secret, а не ConfigMap.

## Сборка

```bash
./gradlew build
```

JAR: `build/libs/panopticum-all.jar`

### Пересборка редакторов (опционально)

Редактор JSON на страницах detail и редакторы запросов используют предсобранные бандлы. Для пересборки после изменения исходных файлов или обновления CodeMirror:

```bash
npm install
npm run build:detail-editor
npm run build:query-editor
```

## Docker

### Из GitHub Container Registry (GHCR)

Образы публикуются автоматически при пуше тега (см. [CI/CD](#cicd)). Запуск:

```bash
docker pull ghcr.io/thesharque/panopticum:latest
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  ghcr.io/thesharque/panopticum:latest
```

Для фиксированной версии используйте тег, например `ghcr.io/thesharque/panopticum:v4.1.0`. Если пакет приватный, сначала выполните вход: `echo $GITHUB_TOKEN | docker login ghcr.io -u ВАШ_ЛОГИН_GITHUB --password-stdin`.

### Из Docker Hub

Образ на [Docker Hub](https://hub.docker.com/r/sharque/panopticum): `sharque/panopticum`

```bash
docker pull sharque/panopticum:latest
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  sharque/panopticum:latest
```

### Сборка образа локально

```bash
docker build -t panopticum:latest .
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  panopticum:latest
```

Откройте **http://localhost:8080**. Для Kubernetes используйте те же переменные окружения и смонтируйте том на `/data` для сохранения данных H2.

## CI/CD

Пуш тега версии (например `v4.1.0`) запускает GitHub Actions: один билд Docker-образа и пуш в:

- [GitHub Container Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry): `ghcr.io/<owner>/panopticum:<tag>`
- [Docker Hub](https://hub.docker.com/r/sharque/panopticum): `<DOCKERHUB_USERNAME>/panopticum:<tag>` (если включено переменной и секретами)

```bash
git tag v4.1.0
git push origin v4.1.0
```

**Docker Hub:** чтобы также пушить в Docker Hub, задайте переменную репозитория `ENABLE_DOCKERHUB` = `true` (Settings → Secrets and variables → Actions → Variables) и добавьте секреты `DOCKERHUB_USERNAME` (логин Docker Hub) и `DOCKERHUB_TOKEN` ([токен](https://hub.docker.com/settings/security)). Без этого workflow выполнится и будет пушить только в GHCR.

Образы будут доступны как `ghcr.io/<your-org>/panopticum:v0.1` и `:latest` (и на Docker Hub как `<DOCKERHUB_USERNAME>/panopticum:v0.1` / `:latest` при настройке). Для деплоя (Kubernetes или pull на сервере) добавьте job или отдельный workflow с нужным тегом образа.
