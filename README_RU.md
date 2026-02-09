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
| **MongoDB** | Просмотр баз и коллекций; выполнение запросов |
| **Redis** | Просмотр баз и ключей; типы и значения |
| **ClickHouse** | Просмотр баз и таблиц; выполнение SQL |
| **Cassandra** | Просмотр keyspace и таблиц; выполнение CQL; редактирование строк (если у таблицы есть primary key) |

Подключения хранятся в H2. В настройках можно добавлять подключения, проверять их и удалять.

## Возможности

- HTTP Basic Auth (учётные данные из env)
- Светлая и тёмная тема (переключатель в шапке и на странице входа)
- Боковая панель со списком сохранённых подключений и быстрым доступом в Настройки
- Добавление, проверка и удаление подключений для каждого типа БД
- Просмотр метаданных (схемы, таблицы, коллекции, ключи) с постраничной навигацией
- Выполнение SQL (PostgreSQL, MySQL/MariaDB, ClickHouse, Cassandra CQL) и запросов (MongoDB)
- Редактирование и сохранение строк в детальном просмотре (PostgreSQL, MySQL при наличии PK/unique, MongoDB, Redis, Cassandra при наличии primary key)
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

## Сборка

```bash
./gradlew build
```

JAR: `build/libs/panopticum-0.1-all.jar`

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
