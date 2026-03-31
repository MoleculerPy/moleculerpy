# MoleculerPy

[![CI](https://github.com/MoleculerPy/moleculerpy/workflows/CI/badge.svg)](https://github.com/MoleculerPy/moleculerpy/actions)
[![PyPI version](https://img.shields.io/pypi/v/moleculerpy.svg)](https://pypi.org/project/moleculerpy/)
[![Python versions](https://img.shields.io/pypi/pyversions/moleculerpy.svg)](https://pypi.org/project/moleculerpy/)
[![codecov](https://codecov.io/gh/MoleculerPy/moleculerpy/graph/badge.svg)](https://codecov.io/gh/MoleculerPy/moleculerpy)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Language / Язык**: [English](README.md) | [Русский](README.ru.md)

MoleculerPy — быстрый, современный и мощный фреймворк для микросервисов на [Python](https://python.org). Помогает строить эффективные, надёжные и масштабируемые сервисы. Python-порт [Moleculer.js](https://moleculer.services) с полной совместимостью по протоколу v4.

**Сайт**: [https://moleculerpy.services](https://moleculerpy.services)

**Документация**: [https://moleculerpy.services/docs](https://moleculerpy.services/docs)

# Что включено

- Нативный async/await (построен на asyncio)
- Концепция запрос-ответ (request-reply)
- Событийная архитектура с балансировкой
- Встроенный реестр сервисов и динамическое обнаружение
- Балансировка нагрузки (round-robin, random, cpu-usage, latency, sharding)
- Паттерны отказоустойчивости (Circuit Breaker, Bulkhead, Retry, Timeout, Fallback)
- Система плагинов/middleware (22 встроенных)
- Подключаемые сериализаторы (JSON, MsgPack) — MsgPack в 2x быстрее на больших данных
- Поддержка потоков для передачи больших данных
- Миксины сервисов
- Встроенное кэширование (Memory, Redis)
- Подключаемые логгеры (structlog)
- Подключаемые транспортеры (NATS, Redis, Memory)
- Валидация параметров
- Несколько сервисов на одном узле
- Безмастерная архитектура — все узлы равны
- Встроенные метрики с экспортёром Prometheus
- Встроенная трассировка с экспортёром Console
- Безопасность протокола v4: проверка версии, обнаружение конфликтов NodeID
- Официальные модули [REPL](https://github.com/MoleculerPy/moleculerpy-repl) и [Channels](https://github.com/MoleculerPy/moleculerpy-channels)

# Установка

```bash
pip install moleculerpy
```

С дополнительными возможностями:
```bash
# С Redis транспортером
pip install moleculerpy[redis]

# С MsgPack сериализатором
pip install moleculerpy[msgpack]

# С метриками и трассировкой
pip install moleculerpy[metrics,tracing]

# Все возможности
pip install moleculerpy[all]
```

# Создайте свой первый микросервис

Этот пример показывает как создать простой сервис с действием `add` для сложения двух чисел.

```python
import asyncio
from moleculerpy import ServiceBroker, Service, action, Context

# Определяем сервис
class MathService(Service):
    name = "math"

    @action
    async def add(self, ctx: Context):
        return ctx.params["a"] + ctx.params["b"]

async def main():
    # Создаём брокер
    broker = ServiceBroker()

    # Регистрируем сервис
    await broker.register(MathService())

    # Запускаем брокер
    await broker.start()

    # Вызываем сервис
    result = await broker.call("math.add", {"a": 5, "b": 3})
    print(f"5 + 3 = {result}")

    # Останавливаем брокер
    await broker.stop()

asyncio.run(main())
```

# С MsgPack сериализатором (в 2x быстрее)

```python
from moleculerpy import ServiceBroker, Settings

broker = ServiceBroker(
    settings=Settings(
        transporter="nats://localhost:4222",
        serializer="msgpack",  # MsgPack вместо JSON
    )
)
```

# Командная строка

MoleculerPy включает CLI для запуска брокера и загрузки сервисов:

```bash
moleculerpy <директория_сервисов> [опции]
```

## Опции CLI

| Опция | Описание | По умолчанию |
|-------|----------|-------------|
| `service_directory` | Путь к директории с файлами сервисов | - |
| `--broker-id, -b` | ID брокера | `node-<имя_директории>` |
| `--transporter, -t` | URL транспортера | `nats://localhost:4222` |
| `--log-level, -l` | Уровень логирования | `INFO` |
| `--log-format, -f` | Формат логов (PLAIN, JSON) | `PLAIN` |
| `--namespace, -n` | Пространство имён сервисов | `default` |

## Пример

```bash
# Запуск с сервисами из директории 'services'
moleculerpy services

# Свой ID брокера и транспортер
moleculerpy services -b my-broker -t nats://nats-server:4222

# Подробное логирование
moleculerpy services -l DEBUG
```

# Официальные модули

| Модуль | Описание |
|--------|----------|
| [moleculerpy-repl](https://github.com/MoleculerPy/moleculerpy-repl) | Интерактивная консоль для отладки и управления сервисами |
| [moleculerpy-channels](https://github.com/MoleculerPy/moleculerpy-channels) | Надёжный pub/sub обмен сообщениями через Redis, Kafka, NATS |

# Middleware

MoleculerPy предоставляет мощную систему middleware для расширения функциональности.

```python
from moleculerpy.middleware import Middleware
from moleculerpy.context import Context

class LoggingMiddleware(Middleware):
    async def local_action(self, next_handler, action_endpoint):
        async def wrapped_handler(ctx: Context):
            print(f"До действия: {action_endpoint.name}")
            result = await next_handler(ctx)
            print(f"После действия: {action_endpoint.name}")
            return result
        return wrapped_handler

# Регистрация middleware
broker = ServiceBroker(middlewares=[LoggingMiddleware()])
```

## Доступные хуки

### Хуки обёртки
- `local_action(next_handler, action_endpoint)` — обёртка локальных обработчиков действий
- `remote_action(next_handler, action_endpoint)` — обёртка удалённых вызовов
- `local_event(next_handler, event_endpoint)` — обёртка обработчиков событий

### Хуки жизненного цикла
- `broker_created(broker)` — после инициализации брокера
- `broker_started(broker)` — после запуска брокера
- `broker_stopped(broker)` — после остановки брокера
- `service_created(service)` — после регистрации сервиса
- `service_started(service)` — после запуска сервиса

# Дорожная карта

## Текущий статус (v0.14.4)
- Полный жизненный цикл сервисов
- Транспортеры: NATS, Redis, Memory
- Подключаемые сериализаторы (JSON, MsgPack) — MsgPack в 2x быстрее на больших данных
- 22 встроенных middleware
- 5 стратегий балансировки нагрузки
- Паттерны отказоустойчивости: Circuit Breaker, Bulkhead, Retry, Timeout, Fallback
- Метрики Prometheus и трассировка Console
- Поддержка потоков
- Безопасность протокола v4: проверка версии, обнаружение конфликтов NodeID
- Модули REPL и Channels
- Pre-commit хуки (ruff format, ruff check, mypy, pytest)

## Планируемые возможности
- Балансированная обработка запросов/событий (NATS queue groups)
- Хук middleware для транзита (transitMessageHandler)
- REST API Gateway (moleculerpy-web)
- TCP транспортер с протоколом Gossip
- Адаптеры баз данных (moleculerpy-db)
- LRU кэш
- Экспортёры трассировки Jaeger и Zipkin
- Версионирование сервисов (v1.users.get)
- Транспортеры Kafka и AMQP
- GraphQL шлюз
- Поддержка gRPC

# Документация

Документация доступна на [https://moleculerpy.services/docs](https://moleculerpy.services/docs).

# Changelog

Смотрите [CHANGELOG.md](CHANGELOG.md).

# Участие в разработке

Мы приглашаем вас присоединиться к разработке MoleculerPy. Пожалуйста, прочитайте наше [руководство для контрибьюторов](CONTRIBUTING.md).

# Благодарности

Проект основан на [pylecular](https://github.com/alvaroinckot/pylecular) от Alvaro Inckot, лицензия MIT.

Вдохновлён [Moleculer.js](https://moleculer.services) — оригинальным Node.js фреймворком для микросервисов.

# Лицензия

MoleculerPy доступен под [лицензией MIT](LICENSE).

# Контакты

Copyright (c) 2026 MoleculerPy

[![@MoleculerPy](https://img.shields.io/badge/github-MoleculerPy-green.svg)](https://github.com/MoleculerPy)
