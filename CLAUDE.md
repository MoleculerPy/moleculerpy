# MoleculerPy — Core Framework

**Python port of Moleculer.js** — fast, modern microservices framework.

## Quick Start

```bash
pip install -e ".[dev]"
pytest                    # Run tests
mypy moleculerpy/        # Type check
ruff check moleculerpy/  # Lint
```

## Architecture

| Component | File | Purpose |
|---|---|---|
| ServiceBroker | broker.py | Central orchestrator |
| Service | service.py | Base service class |
| Context | context.py | Request context |
| Transit | transit.py | Transport layer |
| Registry | registry.py | Service registry |
| Middleware | middleware/ | 22 middleware for resilience |

## Performance

| Scenario | Throughput | Latency |
|---|---|---|
| Local calls | 127,343/sec | 0.008ms |
| NATS | 85,000/sec | 0.011ms |
| Redis | 72,000/sec | 0.014ms |

## Git Workflow

### Branches: main ← dev ← feat/*

```bash
git checkout dev && git pull origin dev
git checkout -b feat/task-name
git add file1 file2                      # NOT "git add ."
git commit -m "feat(module): what was done"
git push origin feat/task-name -u
gh pr create --base dev
gh pr merge --merge --delete-branch=false  # NOT squash!
git checkout dev && git pull origin dev
```

### Commits: `type(module): description`
Types: feat, fix, docs, test, refactor, chore

### Forbidden
- `git push --force`, `git reset --hard`, `git add .`
- Direct commits to main/dev

## Methodology: Route → Shape → Code → Evidence

1. **Route** — Tactical / Standard / Deep / Critical
2. **Shape** — PRD/RFC/ADR before coding (Standard+)
3. **Code** — every function = test immediately, `pytest` required
4. **Evidence** — confirm the result

## Enforcement Hooks

5 hooks in `.claude/hooks/`:

| Hook | Checks |
|---|---|
| forge-safety | Blocks dangerous commands |
| pr-todo-check | P0 checkboxes before PR |
| commit-test-check | Tests for new `def` |
| pre-code-check | Active PRD before code edits |
| pre-commit-health | Blind spots |

## Compatibility

- Protocol v4: 100% compatible with Moleculer.js
- Python: 3.12+
- Transporters: NATS, Redis, Memory
