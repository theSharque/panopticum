# Panopticum

Инструмент для разработчиков и QA — веб-интерфейс для просмотра и управления подключениями к базам данных. Разработан для развёртывания в Kubernetes.

## Стек

- **Backend:** Micronaut 4.x
- **Views:** Thymeleaf + HTMX
- **Auth:** HTTP Basic (логин/пароль из environment)
- **Storage:** H2 (миграции Flyway)

## Возможности

- Авторизация по логину и паролю (Basic Auth)
- Добавление и хранение настроек подключений к PostgreSQL
- Меню со списком подключений и разделом настроек
- Просмотр подключённых БД (MVP: заглушка, в планах — просмотр коллекций и поиск)

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
| `PANOPTICUM_DB_PATH` | Путь к файлу H2 | `./data/panopticum` |

## Сборка

```bash
./gradlew build
```

JAR: `build/libs/panopticum-0.1-all.jar`
