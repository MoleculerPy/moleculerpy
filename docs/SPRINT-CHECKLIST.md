# Sprint Definition of Done — Checklist

**Каждый спринт ОБЯЗАН пройти этот чеклист перед merge.** Невыполненные пункты = technical debt, который фиксируется в KNOWN-ISSUES.md с явным объяснением почему deferred.

## 1. Protocol & Reference Compliance

- [ ] **Node.js reference изучен** — какой код в `sources/reference-implementations/moleculer/` делает то же самое?
- [ ] **Wire format verified** — если менялись packet fields, serializers, топики — сравнено с `.proto` и `packets.js` Node.js
- [ ] **Cross-language tested** — если фича протокольная, запущен реальный Python ↔ Node.js cluster и проверено (хотя бы один smoke test)
- [ ] **ADR создан** если решение архитектурное (A vs B choice) и **включён в PR** (не в родительский репо)
- [ ] **Protocol v4 compliance** — не сломана обратная совместимость с Moleculer.js v4

## 2. Architecture & Design

- [ ] **SRP соблюдён** — каждый класс/модуль имеет одну причину меняться
- [ ] **No private attr access via getattr** — если обращаешься к `_foo`, сделай public property или метод
- [ ] **Type-safe dispatch** — introspection (inspect.signature, isinstance chains) задокументирован как осознанный trade-off
- [ ] **File ownership документирован** — если sprint, явная таблица "кто какой файл трогает"
- [ ] **Нет пересекающихся правок** — агенты не трогают один метод одного файла в одной волне

## 3. Code Quality & Typing

- [ ] **`ruff format` clean**
- [ ] **`ruff check` clean**
- [ ] **`mypy --strict` 0 errors** (кроме pre-existing optional deps warnings)
- [ ] **Никаких `Any` там где можно использовать конкретный тип**
- [ ] **Никаких `getattr()` на известные атрибуты** — только для опциональных optional dependencies
- [ ] **TypedDict / Protocol / dataclass** используется вместо `dict[str, Any]` где возможно
- [ ] **`# type: ignore` имеет комментарий** объясняющий почему
- [ ] **Nullable типы explicit** — `X | None`, не полагаемся на default None

## 4. Testing — Coverage & Quality

### Unit tests
- [ ] **Каждая новая публичная функция имеет тест** (CLAUDE.md rule)
- [ ] **Error paths покрыты** — не только happy path
- [ ] **Edge cases**: None, пустые коллекции, 0, negative numbers, очень большие значения
- [ ] **Mock-based tests помечены как mock-based** — не путать с integration

### Integration tests
- [ ] **Real services** где возможно — Docker brokers (NATS/Redis/Kafka/etc)
- [ ] **Demo matrix прогнан** — 28/28 OK
- [ ] **Demo comprehensive прогнан** — все green
- [ ] **Load/stress test** если фича на hot path

### Regression
- [ ] **Все существующие тесты pass**
- [ ] **Нет skip-in-disguise** — если тест skipped, документирована причина
- [ ] **Codecov patch coverage** — если < 80%, объяснение почему (integration-only paths допустимо)

## 5. Documentation

- [ ] **CHANGELOG обновлён** — Added/Changed/Fixed секции
- [ ] **CLAUDE.md обновлён** если изменения публичного API или архитектуры
- [ ] **Roadmap отражает статус** (`| 0.14.X | ✅ Released | ...`)
- [ ] **Docstrings обновлены** — особенно если изменился signature
- [ ] **ADR создан** для архитектурных решений

## 6. Audit (обязательно для Standard+ масштаба)

- [ ] **Минимум 3 audit agents** запущены параллельно после кода
- [ ] **Все CRITICAL findings исправлены**
- [ ] **Все HIGH findings исправлены** или deferred с обоснованием в KNOWN-ISSUES.md
- [ ] **MEDIUM/LOW findings зафиксированы в TODO** если не фиксятся сейчас
- [ ] **Re-verify после аудит-фиксов** — pipeline pass снова
- [ ] **Security review** для security-sensitive изменений (auth, crypto, serialization)

## 7. Release Readiness

- [ ] **Version bump корректный** — согласован между pyproject.toml, __init__.py, CLAUDE.md
- [ ] **Smoke test**: `pip install .` в чистом venv + `python -c "import moleculerpy; print(__version__)"`
- [ ] **CI green** на PR
- [ ] **Tag создан** после merge в main
- [ ] **Post-release verify** — пакет ставится с PyPI

## 8. Technical Debt Tracking

- [ ] **Deferred items → KNOWN-ISSUES.md** с priority (P0-P3)
- [ ] **"Clever hacks" документированы** — почему именно так, когда refactor
- [ ] **TODO комментарии в коде** имеют ссылку на issue или PRD
- [ ] **Sprint retro sections** в PR description:
  - ✅ Что сделали
  - ⚠️ Что обошли / облегчили
  - ❌ Что не покрыли тестами
  - 📝 Что в technical debt

## 9. Sprint Retro (обязательно в конце спринта)

Перед закрытием sprint ответить на вопросы:

1. **Что мы обошли?** — какие проверки пропустили
2. **Что облегчили?** — где выбрали простое решение вместо правильного
3. **Что не протестировали?** — какие сценарии остались без покрытия
4. **Что странное?** — clever hacks, неочевидные workarounds
5. **Где типизация слабая?** — Any, getattr, Mock без spec, inspect
6. **Что в technical debt?** — что нужно вернуться доделать
7. **Что не соответствует reference?** — отклонения от Node.js Moleculer
8. **Где процесс сломался?** — конфликты агентов, скипнутые шаги аудита

Ответы фиксируются в:
- PR description (секция Sprint Retro)
- memory_retain (Hindsight)
- KNOWN-ISSUES.md (если bugs найдены)
- docs/TODO.md (если debt items)

---

## Red Flags (остановись если видишь)

🚩 **"Unit test с моками достаточно"** — если фича протокольная, нужен real integration test
🚩 **"Codecov soft fail, пропустим"** — если < 70%, обсудить почему
🚩 **"ruff format auto-fixed 10 файлов"** — могли быть агентские конфликты
🚩 **"Я сделал быстрый fix"** — быстрый fix без теста = technical debt
🚩 **"getattr чтобы было гибче"** — нет, это обход типизации
🚩 **"Любой аудит-finding скиплю, это MEDIUM"** — MEDIUM тоже часть качества
🚩 **"Node.js делает так же, наверное"** — прочитай source, не гадай

---

**Версия**: 1.0
**Создан**: 2026-04-08 после ретроспективы Sprint Protocol Lifecycle + Protocol Fixes
