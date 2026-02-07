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
| **PostgreSQL** | Просмотр баз, схем, таблиц; выполнение SQL |
| **MongoDB** | Просмотр баз и коллекций; выполнение запросов |
| **Redis** | Просмотр баз и ключей; типы и значения |
| **ClickHouse** | Просмотр баз и таблиц; выполнение SQL |

Подключения хранятся в H2. В настройках можно добавлять подключения, проверять их и удалять.

## Возможности

- HTTP Basic Auth (учётные данные из env)
- Боковая панель со списком сохранённых подключений и быстрым доступом в Настройки
- Добавление, проверка и удаление подключений для каждого типа БД
- Просмотр метаданных (схемы, таблицы, коллекции, ключи) с постраничной навигацией
- Выполнение SQL (PostgreSQL, ClickHouse) и запросов (MongoDB)
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

Сборка образа:

```bash
docker build -t panopticum:latest .
```

Запуск (данные H2 и учётные данные через env):

```bash
docker run -d --name panopticum \
  -p 8080:8080 \
  -v panopticum-data:/data \
  -e PANOPTICUM_USER=admin \
  -e PANOPTICUM_PASSWORD=changeme \
  panopticum:latest
```

Откройте **http://localhost:8080**. Для Kubernetes используйте те же переменные окружения и смонтируйте том на `/data` для сохранения данных H2.
