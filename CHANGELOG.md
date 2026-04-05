# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.14.13] - 2026-04-05

### Fixed
- **Audit hotfix release** — accumulated fixes from audit cycles:
  - ZipkinExporter: ID truncation to Zipkin spec (16/32 hex), error spans, remoteEndpoint
  - ZipkinExporter: flush() runs HTTP in executor (non-blocking)
  - TracingMiddleware: service name extraction from action (was "unknown")
  - TracingMiddleware: parent-child span linking via ctx.parent_span
  - Tracer.stop(): async support for async exporters
  - broker.mcall(): input validation + options/timeout proxy
  - broker.ping(): transit.connected check + list[str] support
  - health: mem.percent = FREE (Node.js compat), os.uptime, process.argv
  - 35 new tests for broker gaps (mcall, ping, health, $node)

## [0.14.12] - 2026-04-04

### Added
- **$node Internal Service** — 7 introspection actions:
  - `$node.list` — list all cluster nodes
  - `$node.services` — list all services (filter: onlyLocal, skipInternal)
  - `$node.actions` — list all actions
  - `$node.events` — list all events
  - `$node.health` — system health (CPU, memory, OS, process)
  - `$node.options` — broker settings
  - `$node.metrics` — metrics listing
- **broker.mcall()** — parallel multi-action calls (list and dict formats)
  - `settled=True` for partial failure tolerance (like Promise.allSettled)
- **broker.ping()** — ping remote nodes, measure RTT
- **broker.get_health_status()** + `moleculerpy.health` module
  - Returns: cpu, mem, os, process, client, net, time
- Transit: `$node.pong` event emitted on PONG receive (enables broker.ping)

## [0.14.11] - 2026-04-04

### Added
- **Tracing Exporters: Jaeger + Zipkin** (PRD-015)
  - `ZipkinExporter`: batch HTTP POST to Zipkin API v2 (`/api/v2/spans`)
  - `JaegerExporter`: HTTP POST to Jaeger Zipkin collector endpoint
  - Zipkin v2 JSON format with microsecond timestamps
  - Periodic async flush with configurable interval (default 5s)
  - Graceful stop: flush remaining spans before shutdown
  - ID conversion: UUID → 64/128-bit hex (Node.js compatible)
  - Span tags flattened + stringified, annotations from logs
  - `default_tags` merged into every span
  - Zero new dependencies (stdlib `urllib.request`)
  - 58 unit tests + 3 E2E tests

### Fixed
- **Tracer.stop() async**: now properly awaits async exporters (was sync, caused
  RuntimeWarning with ZipkinExporter/JaegerExporter)

## [0.14.10] - 2026-04-04

### Added
- **LRU Memory Cacher** (PRD-014)
  - `MemoryLRUCacher` with bounded size and LRU eviction policy
  - `max` parameter limits cache entries (default: 1000)
  - Least-recently-used items evicted on overflow (O(1) via OrderedDict)
  - `get()` updates item recency — frequently accessed items stay cached
  - `get_with_ttl()` returns `(data, None)` — Node.js compatible
  - Registered as "MemoryLRU" and "memory-lru" in cacher registry
  - Warns on unsupported `lock.staleTime` option (Node.js compat)
  - `max < 1` raises ValueError (prevents infinite eviction loop)
  - 29 unit tests + 3 E2E tests
  - Zero new dependencies (stdlib `collections.OrderedDict`)

## [0.14.9] - 2026-04-03

### Added
- **Pluggable Validator System** (PRD-013)
  - `moleculerpy.validators` package with pluggable validator architecture
  - `BaseValidator` ABC: `init(broker)`, `compile(schema)`, `validate(params, schema)`
  - `BaseValidator.middleware()` returns middleware hooks (Node.js pattern)
  - `BaseValidator.convert_schema_to_moleculer()` for schema format conversion
  - `DefaultValidator` wrapping existing validation logic
  - `resolve_validator()` + `register_validator()` for plugin ecosystem
  - `Settings(validator=...)` broker option: "default", "none", False, class, instance
  - Compile-once-validate-many: `compile()` at action registration, checker at call time
  - Event parameter validation support via middleware hooks
  - 31 unit tests + 12 E2E tests

### Changed
- Broker validation: pre-compiled checker at registration time (was inline per-call)
- Action.__slots__ includes `_compiled_checker` for cached validator
- Broker `_wrap_service_handlers` uses `service.full_name` for action matching

## [0.14.8] - 2026-04-03

### Added
- **Pluggable Metrics Reporter System** (PRD-012)
  - `moleculerpy.metric_reporters` package with pluggable reporter architecture
  - `BaseReporter` ABC: `init(registry)`, `stop()`, `metric_changed()`, includes/excludes filtering
  - `ConsoleReporter`: periodic async metric output to logger (dev mode)
  - `PrometheusReporter`: `get_text()` pull-based Prometheus exposition format
  - `MetricRegistry` extended with `add_reporter()`, `stop_reporters()`, `list()` methods
  - Reporter resolution: `resolve_reporter("console")`, `resolve_reporter({"type": "prometheus"})`
  - `snapshot()` method on Counter, Gauge, Histogram for structured metric export
  - 58 unit tests + 4 E2E tests for reporter system

### Fixed
- **Lock contention in to_prometheus()**: snapshot metric keys under lock, render outside
  lock to avoid blocking registrations during Prometheus scrape
- **ConsoleReporter double-init leak**: cancel existing task before creating new one
- **ConsoleReporter silent no-loop**: log warning when init() called without event loop

## [0.14.7] - 2026-04-03

### Added
- **Service Versioning** (PRD-011)
  - `Service.version` attribute (int or str): `version = 2` → actions as `v2.users.get`
  - `Service.full_name` property: `"v2.users"` for versioned, `"users"` for plain
  - `Service.get_versioned_full_name()` static method (Node.js `service.js:835` compatible)
  - Multiple versions of same service coexist in registry: v1.users + v2.users
  - `$noVersionPrefix` setting suppresses version prefix
  - `$noServiceNamePrefix` setting removes service name prefix from action names
  - INFO packets include `version` and `fullName` fields per service
  - Remote node `fullName` reconstruction for backward compatibility
  - Wire protocol compatible with Node.js Moleculer versioned services

### Fixed
- **Settings propagation bug**: class-level `settings`, `metadata`, `dependencies` on
  services without mixins were not propagated to instances (pre-existing bug found during
  versioning work)
- **INFO action names**: `ensure_local_node()` now uses `handler._name` for action names
  (consistent with registry), not Python method names
- **version=0 handling**: numeric version `0` is now correctly treated as valid (not falsy)

### Changed
- `MixinSchema.version` type: `str` → `int | str` (supports numeric versions)
- Registry keys services by `full_name` instead of `name`

## [0.14.6] - 2026-03-31

### Added
- **Transit Message Handler Hook** (PRD-008)
  - 4th and final transit middleware hook: `transit_message_handler`
  - Intercepts packets after deserialization, before dispatch
  - Use for: auth, rate limiting, audit logging, packet filtering, metrics
  - 4/4 transit middleware hooks now complete (100% Node.js middleware API parity)

## [0.14.5] - 2026-03-31

### Added
- **Balanced Request/Event Handling** (PRD-009)
  - `Settings(disable_balancer=True)` enables native NATS queue group balancing
  - Balanced request topics: `MOL.REQB.<action>` with NATS queue groups
  - Balanced event topics: `MOL.EVENTB.<group>.<event>` with NATS queue groups
  - `prepublish()` routes ALL outgoing packets — balanced vs normal
  - `call_without_balancer()` bypasses broker strategy
  - `make_balanced_subscriptions()` subscribes for all local actions/events
  - Wire protocol compatible with Node.js Moleculer `disableBalancer: true`
  - E2E verified: 49/51 distribution on 100 requests across 2 nodes
- Codecov integration with PR coverage checks
- `codecov.yml` — project target auto, patch target 80%

### Fixed
- Balanced topic parsing: `REQB` → `REQUEST`, `EVENTB` → `EVENT` in packet.py
- Wildcard regex: `re.sub(r"\*\*.*$", ">", event)` matches Node.js behavior
- Disconnected check in `prepublish` — raises `BrokerDisconnectedError`
- `call_without_balancer` guard for `transit=None`

## [0.14.4] - 2026-03-31

### Added
- **Pluggable Serializer System** (PRD-007)
  - `BaseSerializer` ABC with sync/async methods and thread offload (>1MB)
  - `JsonSerializer` — extracted from transporters, proper error handling
  - `MsgPackSerializer` — 2x faster throughput, 40-64% smaller payloads on large data
  - `resolve_serializer()` factory — `Settings(serializer="msgpack")` just works
  - `SerializationError` in error hierarchy
  - `MAX_PAYLOAD_BYTES` (8MB) protection against DoS
  - `pip install moleculerpy[msgpack]` for MsgPack support
- **Transit P0 Safety Fixes** (PRD-006)
  - Protocol version check — reject packets with incompatible `ver`
  - NodeID conflict detection — graceful shutdown on duplicate ID
  - `_SELF_ECHO_TOPICS` exclusion (DISCOVER, HEARTBEAT, INFO)
  - `_shutting_down` guard against repeated `broker.stop()`
  - `ServiceNotFoundError` instead of generic `Exception`
- **Pre-commit hooks** — ruff format + check on commit, mypy + pytest on push
- Demo app `--serializer` flag for JSON/MSGPACK selection

### Fixed
- Middleware order for `transporter_receive` — `reversed()` symmetric with send
- Remote requests/events now use `wrapped_handler` (middleware bypass fix)
- Memory transporter double-deserialization eliminated (sender via meta dict)
- MsgPack `strict_map_key=True` preventing integer key bypass
- JSON deserializer catches `RecursionError` (nested bomb protection)
- Log injection prevention — `%r` for network-supplied data
- 9 pre-existing mypy errors fixed (encryption.py, action_logger.py, context_tracker.py)
- `setattr` → direct assignment in context_tracker.py (ruff B010)

### Changed
- All transporters use pluggable `transit.serializer` instead of hardcoded JSON
- `_broker` typed as `ServiceBroker | None` (was `Any`)
- `_wrapped_publish` typed as `Callable[[Packet], Awaitable[None]] | None` (was `Any`)
- `PROTOCOL_VERSION` constant in transit.py
- Pre-commit ruff updated v0.1.10 → v0.15.0
- 22 middlewares (was 19 in docs)

## [0.14.1] - 2026-01-31

### Added
- Initial public release as MoleculerPy (rebranded from pylecular)
- Full service lifecycle management
- NATS, Redis, and Memory transporters
- 19 built-in middlewares:
  - Circuit Breaker, Bulkhead, Retry, Timeout, Fallback
  - Caching (Memory, Redis)
  - Compression, Encryption
  - Metrics, Tracing, Validator
  - Action Logger, Context Tracker, Error Handler
  - Debounce, Throttle, Hot Reload
- 5 load balancing strategies:
  - Round Robin, Random, CPU Usage, Latency, Sharding
- Built-in metrics with Prometheus exporter
- Built-in tracing with Console exporter
- Streaming support for large data transfer
- Service mixins
- Event-driven architecture with balanced events
- CLI for starting brokers and loading services

### Changed
- Renamed package from `pylecular` to `moleculerpy`
- Updated all imports and references

### Notes
- Version 0.14.1 indicates compatibility with Moleculer Protocol v4 (Moleculer.js 0.14.x)

### Credits
- Based on [pylecular](https://github.com/alvaroinckot/pylecular) by Alvaro Inckot
- Inspired by [Moleculer.js](https://moleculer.services)

## [Unreleased]

### Added
- Domain primitive `NewType` aliases (NodeID, ServiceName, ActionName, EventName, ContextID, RequestID)
- Context kind guards for action/event/internal intent
- Graceful broker shutdown timeout setting
- Wildcard event matching in registry (`*`, `**`)

### Changed
- Broadcast fan-out now follows Moleculer.js `Promise.all` semantics (handler errors propagate)
- Service lifecycle hook dispatch refactored with `match/case` (no behavior change)
- Core models use `__slots__` with extension maps for dynamic attrs (Context, Service, Packet, Registry, Node)

### Fixed
- Registry action/event lookups now indexed (O(1) vs O(N))
- Encryption/compression offloaded for large payloads to avoid event loop blocking
- Transporter disconnects now have timeouts (NATS/Redis)
- Memory transporter task cleanup on disconnect
- Debounce no longer swallows errors (logs warnings)
- Circuit breaker error hierarchy corrected; `RemoteCallError` registered
- Middleware hook gather no longer swallows exceptions
- Streaming cleanup for cancelled streams; pending streams cleared on close
- Silent topic skips in transporters now logged

### Performance
- Reduced allocations in registry event/action selection
- Large JSON deserialization offloaded for >1MB payloads
- Removed task naming overhead in hot paths
- Simplified missing-service scans in broker

### Planned
- Kafka transporter
- AMQP transporter
- REST API Gateway (moleculerpy-web)
- Database adapters (moleculerpy-db)
- GraphQL gateway
- gRPC support
