# Quality Pipeline & Development Methodology

**Этот документ описывает полный цикл разработки MoleculerPy.**
**Краткая версия в [CLAUDE.md](../../CLAUDE.md) → этот файл содержит детали.**

---

## Quality Pipeline

**ОБЯЗАТЕЛЬНЫЙ pipeline для КАЖДОЙ фичи/фикса перед PR:**

```
 1. CODE       — реализация + unit тесты (каждая функция = тест)
 2. FORMAT     — ruff format moleculerpy/
 3. LINT       — ruff check moleculerpy/ → 0 errors
 4. TYPES      — mypy moleculerpy/ --strict → 0 errors
 5. UNIT       — pytest tests/unit/ → all pass
 6. E2E        — pytest tests/e2e/ → all pass (MemoryTransporter)
 7. INTEG      — pytest tests/integration/ → all pass (real NATS, если доступен)
 8. AUDIT      — минимум 2 агента (code-review + security)
 9. FIX        — исправить ВСЕ HIGH/CRITICAL findings из аудита
10. RE-VERIFY  — ПОВТОРИТЬ шаги 2-7 ПОСЛЕ фиксов (не доверять предыдущему прогону!)
11. RE-AUDIT   — если были значительные фиксы → ещё раунд аудита
12. SMOKE      — python -m build && pip install в чистый venv → import + version
13. PR         — только после шагов 1-12
```

### Quick commands

```bash
# Шаги 2-5 одной строкой:
ruff format moleculerpy/ && ruff check moleculerpy/ && mypy moleculerpy/ && pytest tests/unit/

# E2E (без Docker):
pytest tests/e2e/ -v

# Integration (нужен NATS на localhost:4222):
pytest tests/integration/ -v

# Smoke test:
python -m build
python3.12 -m venv /tmp/smoke && /tmp/smoke/bin/pip install dist/*.whl
/tmp/smoke/bin/python -c "import moleculerpy; print(moleculerpy.__version__)"
rm -r /tmp/smoke
```

---

## Evidence Table

**Обязательна в каждом PR body:**

```markdown
| Layer | Tests | Result |
|---|---|---|
| Unit | N passed, M skipped | ✅/❌ |
| E2E | N passed | ✅/❌ |
| Integration (NATS) | N passed | ✅/❌/⏭️ skipped |
| mypy strict | N files | ✅/❌ |
| ruff | — | ✅/❌ |
| Audit | N agents, N findings | ✅ fixed / ❌ open |
| Smoke | pip install | ✅/❌ |
```

---

## Evidence Levels (FPF B.5.1)

| Level | Что включает | Когда достаточно |
|---|---|---|
| L0 | "Я думаю работает" | **Никогда** |
| L1 | Unit тесты с моками | Tactical (typo, 1 строка) |
| L2 | E2E + integration + real transport | Standard (фича, PR) |
| L3 | Demo app + cross-language interop | Deep (новый модуль, релиз) |

**Правило: для релиза (tag + PyPI) нужен минимум L2 по каждой новой фиче.**

---

## Forgeplan Integration

**Все задачи Standard+ ДОЛЖНЫ иметь PRD в `.forgeplan/prds/` перед началом кода.**

### Workflow

```
1. Проверить .forgeplan/ROADMAP.md — где эта задача в roadmap?
2. Если PRD не существует → создать (изучив sources/ reference сначала!)
3. PRD ОБЯЗАН содержать:
   - Reference на Node.js source (файл:строка)
   - FR-требования (FR-001, FR-002, ...)
   - Evidence criteria (что тестировать)
4. После завершения → PRD status: done
5. Если задача слишком сложная → декомпозировать в forgeplan
```

### Reference-first development

- **ВСЕГДА** изучай `sources/reference-implementations/moleculer/` перед реализацией
- Сравнивай Python реализацию с Node.js/Go
- Документируй расхождения в PRD

---

## Audit Cycle

```
Round 1:
  → 2+ агента (code-review + security/architect)
  → Собрать findings → классифицировать:
    CRITICAL → fix немедленно
    HIGH     → fix в этом PR
    MEDIUM   → fix если просто, иначе → forgeplan как отдельный PRD
    LOW      → document only

Round 2 (после фиксов):
  → Re-run: FORMAT → LINT → TYPES → UNIT → E2E
  → Если были значительные фиксы → ещё один аудит-агент
  → Repeat until: 0 CRITICAL + 0 HIGH

Pre-existing issues:
  → НЕ блокируют PR если задокументированы
  → Добавить в KNOWN-ISSUES.md или forgeplan
```

---

## Anti-patterns

| Плохо | Почему | Вместо этого |
|-------|--------|-------------|
| Код без PRD (Standard+) | Непонятно зачем | PRD в forgeplan → потом код |
| Тесты "потом" | Потом = никогда | Тест сразу после функции |
| "Работает — значит готово" | Нет evidence | Evidence Table в PR |
| Squash merge | Теряет поздние коммиты | Merge commit |
| `git add .` | Добавит секреты | `git add файл1 файл2` |
| Аудит без re-verify | Фиксы могут сломать | RE-VERIFY после каждого фикса |
| Только unit тесты для релиза | Моки врут | Минимум L2 (E2E + NATS) |
| Код без reference | Не совместимо с Node.js | Изучи sources/ ПЕРЕД кодом |
| PRD без FR-номеров | Нет критерия "готово" | FR-001 + evidence criteria |
| Забыл обновить forgeplan | Потеря контекста | PRD status: done после merge |

---

## Release Checklist

```bash
# 1. Все шаги Quality Pipeline пройдены
# 2. Version bump:
grep -rn "X.Y.Z" pyproject.toml moleculerpy/__init__.py README.md CHANGELOG.md

# 3. Release:
git checkout dev && git pull
git checkout -b release/vX.Y.Z
# ... final tests, smoke ...
gh pr create --base main
gh pr merge --merge
git checkout main && git pull
git tag -a vX.Y.Z -m "Release vX.Y.Z: описание"
git push origin vX.Y.Z
# PyPI publish triggers on tag

# 4. Sync:
git checkout dev && git merge main && git push origin dev

# 5. Post-publish verify:
pip install moleculerpy==X.Y.Z --force-reinstall
python -c "import moleculerpy; print(moleculerpy.__version__)"
```
