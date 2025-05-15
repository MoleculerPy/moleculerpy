# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
