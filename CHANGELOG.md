# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
