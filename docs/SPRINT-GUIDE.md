# MoleculerPy Sprint Guide

**Формализованный sprint cycle для каждой фичи/релиза.**
**Ссылка из CLAUDE.md. Используется агентами, командами, скиллами.**

---

## Sprint Cycle (5 фаз)

Каждая фича проходит ровно 5 фаз. Нельзя пропускать.

```
EXPLORE → ADI → CODE → TESTS → PIPELINE → RELEASE
```

### Phase 1: EXPLORE

**Цель**: понять ЧТО делать и КАК это сделано в Node.js reference.

```
Шаги:
1. /recall — восстановить контекст из Hindsight
2. Прочитать PRD из .forgeplan/prds/PRD-XXX.md
3. Изучить Node.js reference: sources/reference-implementations/moleculer/src/...
   - Прочитать ВЕСЬ файл (не grep, а Read полностью)
   - Документировать: конструктор, методы, параметры, LOC
4. Изучить текущее Python состояние:
   - Что уже есть? Что переиспользовать?
   - Какой base class? Какие imports?
5. Сформулировать план:
   - Файлы для создания/изменения
   - Estimated LOC
   - Key decisions (data structure, naming, backward compat)
6. Результат: sprint breakdown с задачами
```

**Артефакты**: research summary, sprint breakdown, team plan

### Phase 1.5: ADI Checkpoint (Abduction → Deduction → Induction)

**Цель**: перед кодом убедиться что выбран лучший подход из возможных.

```
FPF B.5 Reasoning Cycle — ADI:

Abduction (гипотезы):
  "Какие подходы возможны? Что делает Node.js? Что делает Go? Есть ли Python-идиома лучше?"

Deduction (выбор):
  "Какой подход лучше для нашего проекта? Почему?"

Induction (подтверждение):
  "Подтвердится ли выбор тестами и audit? Сверка с reference."
```

**Когда обязателен:**

| Scale | ADI | Почему |
|---|---|---|
| Tactical | Нет | Один очевидный подход |
| Standard | Рекомендуется | 2-3 подхода, стоит проверить |
| Deep | **Обязателен** | Необратимо, нужны 3+ гипотезы |
| Critical | **Обязателен + review** | Кросс-модуль, adversarial review |

**Как делать (Standard):**
1. Назови 2-3 подхода к реализации
2. Для каждого: плюсы, минусы, effort
3. Выбери один, запиши почему в sprint plan
4. Если все подходы сходятся → кодь уверенно
5. Если есть конкурирующие → обсуди с пользователем

**Пример из реальной практики:**
- v0.14.8 metrics: "metric_reporters/ vs metrics/ package?" → metrics/ конфликтовал с metrics.py → выбрали metric_reporters/
- v0.14.9 validators: "inline validate vs middleware pattern?" → audit показал что Node.js использует middleware → переделали

**Артефакты**: обоснование выбора подхода (в sprint plan или PR description)

### Phase 2: CODE

**Цель**: реализация по Node.js паттерну с Python идиомами.

```
Правила:
1. Reference-first — каждый метод сверяется с Node.js source
2. Naming: Python snake_case, но API совместимо с Node.js
3. Backward compat: существующие модули НЕ ломать (re-exports)
4. Types: все public API с type hints (mypy strict)
5. No new deps если можно (stdlib first)
6. __init__.py: register в registry, добавить в __all__
```

**Team pattern** (для Standard+):
```
TeamCreate("sprint-vX.Y.Z-feature")
  ├─ backend-dev (sonnet, bypassPermissions) → реализация
  ├─ test-writer (sonnet, bypassPermissions) → тесты (параллельно если API известен)
  └─ audit (opus, background) → reference comparison
```

**Артефакты**: новые файлы, изменённые файлы

### Phase 3: TESTS

**Цель**: минимум L2 evidence (unit + E2E).

```
Unit tests (минимум 20):
  - Constructor/init
  - Core methods (happy path)
  - Edge cases (empty, None, overflow)
  - Error handling (ValidationError, ValueError)
  - Backward compat imports
  - Resolve by name/class/instance

E2E tests (минимум 3):
  - Broker с фичей работает end-to-end
  - Фича под нагрузкой / overflow
  - Backward compat (старое поведение не сломано)

Integration tests (если есть транспорт):
  - Real NATS (если фича затрагивает wire protocol)
```

**Артефакты**: test files, evidence counts

### Phase 4: PIPELINE

**Цель**: 0 errors, 0 HIGH findings, 80%+ patch coverage.

```
Обязательная последовательность (нельзя пропускать):

1. FORMAT     ruff format moleculerpy/
2. LINT       ruff check moleculerpy/           → 0 errors
3. TYPES      mypy moleculerpy/                 → 0 errors
4. UNIT       pytest tests/unit/                → all pass
5. E2E        pytest tests/e2e/                 → all pass
6. AUDIT      2+ агента:
               - code-reviewer (sonnet) — quality, thread safety, edge cases
               - reference-checker (opus) — сверка с Node.js source file:line
7. FIX        Исправить ВСЕ HIGH/CRITICAL findings
8. RE-VERIFY  Повторить шаги 1-5 ПОСЛЕ фиксов
9. SMOKE      python -m build → pip install в чистый venv → import + version

Codecov: patch coverage >= 80% (если ниже — добавить тесты)
```

**Артефакты**: Evidence Table для PR

### Phase 5: RELEASE

**Цель**: tag на PyPI, все docs обновлены, память сохранена.

```
Последовательность (строго):

1. VERSION BUMP (4 файла, ОДНО число):
   - pyproject.toml          → version = "X.Y.Z"
   - moleculerpy/__init__.py → __version__ = "X.Y.Z"
   - README.md               → ## Current status (vX.Y.Z)
   - CHANGELOG.md            → ## [X.Y.Z] - YYYY-MM-DD

2. GIT FLOW:
   git checkout dev && git pull
   git checkout -b feat/feature-name
   git add <specific files>
   git commit -m "feat(module): description — PRD-XXX"
   git push origin feat/feature-name -u
   gh pr create --base dev (с Evidence Table в body)
   gh pr merge --merge --delete-branch=false

3. RELEASE:
   git checkout dev && git pull
   git checkout -b release/vX.Y.Z
   git push origin release/vX.Y.Z -u
   gh pr create --base main
   gh pr merge --merge --delete-branch=false
   git checkout main && git pull
   git tag -a vX.Y.Z -m "Release vX.Y.Z: description"
   git push origin vX.Y.Z  ← triggers PyPI publish

4. SYNC:
   git checkout dev && git merge main && git push origin dev

5. CLOSE OUT (ВСЕ обязательны):
   - .forgeplan/prds/PRD-XXX.md → status: done
   - .forgeplan/ROADMAP.md → version в Released, Next сдвинуть
   - CLAUDE.md → Current Focus, Version, Compatibility table
   - docs/TODO.md → [x] version marked done
   - Hindsight → memory_retain() с release notes
   - TeamDelete() если была команда
```

**Артефакты**: PR URLs, tag, updated docs

---

## Evidence Table Template

```markdown
| Layer | Tests | Result |
|---|---|---|
| Unit | N passed, M skipped | ✅/❌ |
| E2E | N passed | ✅/❌ |
| Integration (NATS) | N passed | ✅/❌/⏭️ |
| mypy strict | N files | ✅/❌ |
| ruff | — | ✅/❌ |
| Audit | N agents, N findings | ✅ fixed / ❌ open |
| Codecov | N% patch | ✅ ≥80% / ❌ |
| Smoke | pip install | ✅/❌ |
```

---

## Commit Message Format

```
type(module): what was done — short description

Detailed explanation in Russian.

- bullet points of changes
- N unit + M E2E tests, TOTAL pass
- Audit: N agents, N findings fixed

Refs: PRD-XXX

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`

---

## Team Recipes by Scale

| Scale | Team Size | Agents | Pattern |
|---|---|---|---|
| Tactical | 1-2 | backend-dev + test-writer | Parallel, no audit |
| Standard | 3 | backend-dev + test-writer + audit(opus) | Parallel code+tests, sequential audit |
| Deep | 4+ | backend + test + audit + research | Sequential phases, plan approval |

---

## Checklist (copy for each sprint)

```
[ ] EXPLORE: PRD read, Node.js source studied, sprint plan written
[ ] ADI: alternatives considered, approach justified (Standard+ only)
[ ] CODE: files created, imports work, existing tests pass
[ ] TESTS: unit (20+) + E2E (3+) written and passing
[ ] FORMAT: ruff format — 0 reformatted
[ ] LINT: ruff check — 0 errors
[ ] TYPES: mypy strict — 0 errors
[ ] UNIT: all pass
[ ] E2E: all pass
[ ] AUDIT: 2+ agents, 0 HIGH/CRITICAL remaining
[ ] RE-VERIFY: all steps 1-5 re-run after fixes
[ ] SMOKE: pip install + import + version OK
[ ] VERSION: 4 files bumped to same version
[ ] PR: Evidence Table in body, merged to dev
[ ] RELEASE: tag pushed, PyPI triggered
[ ] SYNC: dev merged with main
[ ] FORGEPLAN: PRD → done, ROADMAP updated
[ ] DOCS: CLAUDE.md, TODO.md, CHANGELOG updated
[ ] MEMORY: Hindsight saved
[ ] TEAM: cleaned up
```
