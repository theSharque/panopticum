# Panopticum

Инструмент для разработчиков и QA — веб-интерфейс для просмотра и управления подключениями к базам данных. Предназначен для развёртывания в Kubernetes или Docker.

## Стек

- **Backend:** Micronaut 4.x
- **Views:** Thymeleaf + HTMX
- **Auth:** HTTP Basic (логин/пароль из environment)
- **Storage:** H2 (миграции Flyway)
- **i18n:** английский и русский

## Поддерживаемые БД

| Тип | Возможности |
|-----|--------------|
| **PostgreSQL** | Просмотр баз, схем, таблиц; выполнение SQL; редактирование строк |
| **MySQL (MariaDB)** | Просмотр баз и таблиц; выполнение SQL; редактирование строк (если у таблицы есть PK или unique-индекс) |
| **MS SQL Server** | Просмотр баз, схем, таблиц; выполнение SQL; редактирование строк (если у таблицы есть PK или unique-индекс) |
| **Oracle Database** | Просмотр схем и таблиц; выполнение SQL; редактирование строк по ROWID |
| **MongoDB** | Просмотр баз и коллекций; выполнение запросов |
| **Redis** | Просмотр баз и ключей; типы и значения |
| **ClickHouse** | Просмотр баз и таблиц; выполнение SQL |
| **Cassandra** | Просмотр keyspace и таблиц; выполнение CQL; редактирование строк (если у таблицы есть primary key) |
| **RabbitMQ** | Просмотр очередей; просмотр сообщений (peek, только чтение, без редактирования) |
| **Kafka** | Просмотр топиков и партиций; просмотр записей (только чтение) |

Подключения хранятся в H2. В настройках можно добавлять подключения, проверять их и удалять.

## Возможности

- HTTP Basic Auth (учётные данные из env)
- Светлая и тёмная тема (переключатель в шапке и на странице входа)
- Боковая панель со списком сохранённых подключений и быстрым доступом в Настройки
- Добавление, проверка и удаление подключений для каждого типа БД
- Просмотр метаданных (схемы, таблицы, коллекции, ключи) с постраничной навигацией
- Выполнение SQL (PostgreSQL, MySQL/MariaDB, MS SQL Server, Oracle, ClickHouse, Cassandra CQL) и запросов (MongoDB)
- Редактирование и сохранение строк в детальном просмотре (PostgreSQL по ctid, MySQL/MS SQL Server при наличии PK/unique, Oracle по ROWID, MongoDB, Redis, Cassandra при наличии primary key)
- HTMX для частичного обновления без перезагрузки страницы
- Локализация: EN и RU (по браузеру или пути)

## Запуск

```bash
./gradlew run
```

Приложение: **http://localhost:8080**

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

1. **Явные поля:** `name`, `type`, `host`, `port`, `database`, `username`, `password`. Поддерживаемые значения `type`: `postgresql`, `mongodb`, `redis`, `clickhouse`, `mysql`, `sqlserver`, `oracle`, `cassandra`, `rabbitmq`, `kafka`.
2. **JDBC-строка:** поля `name` и `jdbcUrl` (или `url`). По URL извлекаются тип, хост, порт, база, пользователь и пароль. Поддерживается для PostgreSQL, MySQL, MS SQL Server, Oracle и ClickHouse (например `jdbc:postgresql://user:pass@host:5432/dbname`, `jdbc:sqlserver://host:1433;databaseName=db;user=sa;password=secret`, `jdbc:oracle:thin:@//host:1521/XEPDB1`).

Пример:

```json
[
  {"name": "prod-pg", "jdbcUrl": "jdbc:postgresql://app:secret@pg.svc:5432/mydb"},
  {"name": "analytics", "jdbcUrl": "jdbc:clickhouse://clickhouse.svc:8123/default"},
  {"name": "cache", "type": "redis", "host": "redis.svc", "port": 6379}
]
```

Для Helm: поместите JSON в Secret и смонтируйте его в переменную окружения `PANOPTICUM_CONNECTIONS_JSON` (например через `valueFrom.secretKeyRef`). Для JSON с паролями используйте Secret, а не ConfigMap.

## Сборка

```bash
./gradlew build
```

JAR: `build/libs/panopticum-all.jar`

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

## RabbitMQ (локальная разработка)

Поддержка RabbitMQ реализована через **Management HTTP API** (только чтение: список очередей, просмотр сообщений через peek). AMQP-клиента в приложении нет.

### Запуск RabbitMQ с Management-плагином

Из корня проекта:

```bash
docker compose up -d rabbitmq
```

- AMQP: **localhost:43009**
- Веб-интерфейс и Management API: **http://localhost:43010** (логин/пароль по умолчанию: `guest` / `guest`)

### Создание тестовой очереди и отправка сообщений

1. Откройте http://localhost:43010 и войдите под `guest` / `guest`.
2. Перейдите в **Queues** → **Add a new queue** (например имя `test-queue`, vhost `/`) → **Add queue**.
3. Откройте очередь → **Publish message** и отправьте несколько сообщений (payload и routing key по желанию).

Либо через Management API (очередь создаётся вручную во вкладке Queues, затем публикация):

```bash
curl -u guest:guest -X POST http://localhost:43010/api/exchanges/%2F/amq.default/publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"test-queue","payload":"{\"hello\":\"world\"}","payload_encoding":"string"}'
```

(Нужно, чтобы очередь `test-queue` была привязана к default exchange или к выбранному.)

### Добавление подключения RabbitMQ в Panopticum

1. **Настройки** → выберите **RabbitMQ** → укажите **Хост** (например `localhost`), **Порт** (порт Management API, например `43010` или `15672`), **VHost** (например `/`), **Пользователь** и **Пароль**.
2. Нажмите **Проверить**, затем **Добавить**. Подключение появится в боковой панели.
3. Откройте его → **Очереди** (список) → выберите очередь → **Сообщения** (peek, только чтение) → выберите сообщение → **Сообщение** (детальный просмотр, только чтение).

Экраны: **Очереди** → **Сообщения** (по одной очереди) → **Сообщение** (деталь). Редактирование и удаление сообщений недоступны.

## Kafka (локальная разработка)

Поддержка Kafka реализована через **нативный Java-клиент** (AdminClient + Consumer). Только чтение: список топиков, список партиций, просмотр записей в партиции. Без публикации и удаления.

### Запуск Kafka и загрузка тестовых данных

Из корня проекта:

```bash
docker compose up -d kafka kafka-init
```

- Bootstrap: **localhost:43011**
- Сервис `kafka-init` создаёт топики `demo-events` (3 партиции) и `demo-metrics` (1 партиция) и записывает тестовые JSON-сообщения.

### Добавление подключения Kafka в Panopticum

1. **Настройки** → выберите **Kafka** → укажите **Хост** (например `localhost`), **Порт** (например `43011`).
2. Нажмите **Проверить**, затем **Добавить**. Подключение появится в боковой панели.
3. Откройте его → **Топики** (список) → выберите топик → **Партиции** → выберите партицию → **Записи** (peek) → выберите запись → **Запись** (детальный просмотр, только чтение).

Экраны: **Топики** → **Партиции** (по одному топику) → **Записи** (по одной партиции) → **Запись** (деталь). Редактирование и удаление недоступны.

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
