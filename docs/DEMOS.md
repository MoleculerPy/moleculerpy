# MoleculerPy Demo Stands

This document describes the component-level demo stands built during the
`sprint-component-demos` sprint. Each stand verifies a real component of
MoleculerPy against real services (Docker brokers), not mocks.

## Quick reference

| Demo | Component | Tests | Services | Duration |
|---|---|---|---|---|
| `demo_matrix` | Transporters × Serializers | 28/28 | NATS, Redis, MQTT, RabbitMQ, Kafka | ~45s |
| `demo_comprehensive` | Protocol v4 features | 90/90 | NATS, Redis | ~90s |
| `demo_crosslang` | Python ↔ Node.js interop | 5/5 | NATS + Node.js | ~12s |
| `demo_cacher` | Memory / LRU / Redis cachers | 7/7 | Redis (port 6381) | ~4s |
| `demo_channels` | Pub/Sub middleware | 6/6 | Redis, NATS | ~7s |
| `demo_repl` | REPL command dispatcher | 10/10 | NATS (optional) | ~5s |
| `demo_web` | HTTP API Gateway | 11/11 | none (in-proc) | ~8s |
| `demo_observability` | Logging / Metrics / Tracing | 9/9 | none (in-proc) | ~3s |

**Total:** 8 demos, 166 tests, ~3 min end-to-end.

## Running

### All demos
```bash
python examples/run_all_demos.py
```

Prints pre-flight Docker broker check, runs each demo sequentially, and
prints a unified sweep table. Exits 0 iff every selected demo passed.

### Subsets
```bash
python examples/run_all_demos.py --quick          # skip demo_comprehensive
python examples/run_all_demos.py --only demo_cacher
python examples/run_all_demos.py --skip demo_web --skip demo_repl
python examples/run_all_demos.py --list           # print registry and exit
```

### Individually
Every demo is a standalone script:
```bash
python moleculerpy/examples/demo_matrix.py
python moleculerpy/examples/demo_comprehensive.py
python moleculerpy/examples/demo_crosslang.py
python moleculerpy/examples/demo_cacher.py
python moleculerpy/examples/demo_observability.py
python examples/demo_channels.py
python examples/demo_repl.py
python examples/demo_web.py
```

Each exits 0 on full pass, 1 otherwise, and prints a colored PASS/FAIL
table at the bottom.

## Pre-flight requirements

The orchestrator calls `docker ps` and reports which brokers are running.
The check is advisory — missing brokers don't block the run, the affected
demos will just fail with a clear error.

| Broker | Default port | Start command |
|---|---|---|
| NATS | 4222 | `docker run -d -p 4222:4222 nats:2.10-alpine` |
| Redis/Valkey | 6381 | `docker run -d -p 6381:6379 valkey/valkey:7-alpine` |
| MQTT (Mosquitto) | 1883 | `docker run -d -p 1883:1883 eclipse-mosquitto:2` |
| RabbitMQ | 5672 | `docker run -d -p 5672:5672 rabbitmq:3-alpine` |
| Kafka | 9092 | `docker run -d -p 9092:9092 confluentinc/cp-kafka:7.5.0` |

Optional Python packages (per demo):
- `demo_channels` → `moleculerpy-channels` (installed via `pip install -e moleculerpy-channels[all]`)
- `demo_repl` → `moleculerpy-repl`
- `demo_web` → `moleculerpy-web`, `httpx`
- `demo_crosslang` → Node.js runtime, `moleculer` npm package in `tests/integration/node_services`

If a package is missing, the demo exits with a clear install hint (exit 2).

## What each demo verifies

### demo_matrix — Transports × Serializers (28/28)
**File:** `moleculerpy/examples/demo_matrix.py`
**Real services:** NATS + Redis + MQTT + RabbitMQ + Kafka.

Sweeps every supported `transporter × serializer` combination (JSON,
MsgPack, CBOR, ProtoBuf) to confirm end-to-end RPC works on every pair.
Used as the canonical smoke test before cutting a release. "PASS" = RPC
round-trip with the matching serializer completed on that transporter.

### demo_comprehensive — Protocol v4 features (90/90)
**File:** `moleculerpy/examples/demo_comprehensive.py`
**Real services:** NATS + Redis.

Broadest feature sweep: service discovery, heartbeats, load balancing
strategies, middleware chain, circuit breaker, bulkhead, retry, timeout,
fallback, caching, validation, versioning, events (broadcast/emit),
streaming, metrics, tracing. Longest demo — skipped by `--quick`.

### demo_crosslang — Python ↔ Node.js (5/5)
**File:** `moleculerpy/examples/demo_crosslang.py`
**Real services:** NATS + Node.js `crosslang_test.service.js`.

Cross-language interoperability: Python calls Node.js actions (T1),
Node.js calls Python actions (T3, verified via file-based feedback),
events flow both directions (T2, T4), and graceful stop is observed on
the Node side (T5). Uses `/tmp/crosslang_test_*.log` files as an
out-of-band feedback channel.

**Known caveat (T4 payload gap):** the event delivery test verifies
arrival but does a shallow payload check — deep schema equality is left
to `demo_comprehensive` and the integration test suite.

### demo_cacher — Memory + LRU + Redis (7/7)
**File:** `moleculerpy/examples/demo_cacher.py`
**Real services:** Redis on `localhost:6381` (db 15 for isolation).

Covers `MemoryCacher` get/set/delete/TTL, `MemoryLRUCacher` eviction,
`RedisCacher` round-trip with TTL, the `@cache` action middleware,
concurrent 100-way get/set, pattern-based `clean()`, and `getWithTTL`
remaining-time retrieval. Flushes db 15 between tests.

### demo_channels — Pub/Sub (6/6)
**File:** `examples/demo_channels.py`
**Real services:** Redis (6381) + NATS (4222).

`moleculerpy-channels` middleware: basic publish/subscribe, consumer
group balancing, DLQ on repeated failure, retry policy (success on 3rd
attempt), graceful shutdown with in-flight drain, and the NATS adapter
running the same suite.

### demo_repl — REPL commands (10/10)
**File:** `examples/demo_repl.py`
**Real services:** in-process broker (NATS optional).

Programmatic smoke test of the `moleculerpy-repl` command dispatcher:
`actions`, `call`, `broadcast`, `list`, `info`, `ping`, `metrics`,
`cache`, `listener`, `quit`. Bypasses the interactive prompt_toolkit UI
by invoking command handlers directly.

### demo_web — HTTP gateway (11/11)
**File:** `examples/demo_web.py`
**Real services:** in-process broker + `httpx.AsyncClient` (real HTTP
over a loopback port).

End-to-end HTTP tests of `moleculerpy-web`: GET list / GET one / POST
create / 404 / 400 validation / CORS preflight / custom auth middleware
/ ETag + 304 / query params / streaming / graceful shutdown with
in-flight request drain.

### demo_observability — Logging + Metrics + Tracing (9/9)
**File:** `moleculerpy/examples/demo_observability.py`
**Real services:** in-process (no Docker).

Three pillars, three tests each. Logging: structured log capture,
level filter, service-scoped logger. Metrics: console reporter counter,
Prometheus exposition format, custom gauge. Tracing: console exporter
nested spans, event exporter span events, span attributes.

Jaeger/Zipkin/Datadog exporters are out of scope here — they need their
own Docker stacks and are covered by integration tests.

## When to use which demo

| Goal | Use |
|---|---|
| Fast smoke after a local change | `--only demo_cacher` or `--only demo_observability` |
| Protocol regression check | `demo_comprehensive` |
| Release gate (broker matrix) | `demo_matrix` + `demo_comprehensive` |
| Node.js interop regression | `demo_crosslang` |
| Full confidence before cutting a tag | `run_all_demos.py` (no flags) |
| CI quick lane (skips longest) | `run_all_demos.py --quick` |

## Output parsing

The orchestrator parses the last ~40 stdout lines of each demo for a
summary in one of these forms (case-insensitive, ANSI-stripped):

- `N/M tests passed`, `N/M passed`
- `Passed: N  |  Failed: K`  (→ `N / (N+K)`)
- `Total: N  |  OK: M`       (→ `M/N`)
- `Total: N/M passed`

If a demo uses a different format, add a new pattern to
`SUMMARY_PATTERNS` in `examples/run_all_demos.py` — the orchestrator
falls back to `"-"` for the Result column but still honors the child
exit code for the Status column.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | All selected demos passed |
| 1 | At least one demo failed, timed out, or was missing |
| 2 | (per-demo) optional dependency missing — surfaced as FAIL in the table |

## Adding a new demo

1. Write `examples/demo_<name>.py` (or `moleculerpy/examples/...` for
   core-only demos). Follow the existing structure: pre-flight check,
   list of async test functions, colored PASS/FAIL table, exit 0/1.
2. Make sure the final line contains `N/M passed` or a supported
   summary format.
3. Append a `Demo(...)` entry to `DEMOS` in
   `examples/run_all_demos.py` with the expected result and timeout.
4. Add a row to the table at the top of this file.
