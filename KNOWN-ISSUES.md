# Known Issues & Technical Debt

Tracker for deferred fixes, clever hacks, and known gaps. Every deferred audit finding lands here with priority and context.

**Priority**:
- **P0**: production risk, fix ASAP
- **P1**: important, fix in next sprint
- **P2**: quality/debt, fix when touching area
- **P3**: nice-to-have, backlog

---

## P1 — Fix in next sprint

### 1. seq++ re-registration guard missing

**File**: `moleculerpy/broker.py:783-785` — `register()` method
**Description**: `local_node.seq += 1` runs unconditionally. If `register()` is called twice for the same service (hot reload, test teardown+reregister), seq increments twice causing spurious INFO broadcasts and remote endpoint table rebuilds.
**Discovered**: Sprint Protocol Fixes audit, broker-auditor MEDIUM-4
**Fix**: Check if service already registered via `self.registry.__services__` before seq++
**Effort**: ~15 min

### 2. `inspect.signature()` not cached in hook dispatch

**File**: `moleculerpy/broker.py:300-311` — `_alias_args()` helper
**Description**: Runs per-middleware per-hook dispatch. Not on hot path (lifecycle only), but avoidable.
**Discovered**: Sprint Protocol Fixes audit, broker-auditor MEDIUM-3
**Fix**: Cache signature param count in dict `{(id(mw), method_name): param_count}` at first call
**Effort**: ~30 min

### 3. No real Python ↔ Node.js ProtoBuf cluster test

**Description**: We claim cross-language ProtoBuf interop works via proto3 unknown-field semantics. This is proto spec behavior, but we never verified with a real Node.js broker running alongside a Python broker.
**Discovered**: Sprint retro 2026-04-08
**Fix**: Add e2e test spawning real Node.js Moleculer broker + Python broker, verify they discover each other and can call actions
**Effort**: ~3h (setup Node.js toolchain, Docker compose)

### 4. ProtoBuf regeneration has no CI guard

**Description**: `packets_pb2.py` is regenerated manually from `packets.proto`. No Makefile target, no CI check that pb2 is in sync with proto.
**Discovered**: Sprint Protocol Fixes audit, protocol-auditor LOW-1
**Fix**: Add CI step that runs protoc and diffs against checked-in pb2
**Effort**: ~1h

---

## P2 — Quality debt, fix when touching area

### 5. `_was_connected` private attribute access via getattr

**File**: `moleculerpy/broker.py:789` — `register()` method
**Description**: `getattr(self.transit, "_was_connected", False)` — private attr access via getattr bypasses type checker
**Fix**: Add `is_connected` public property to Transit class
**Effort**: ~15 min

### 6. broker.py is a God Object (12+ responsibilities)

**Description**: After Transit SRP sprint, broker.py still holds: lifecycle, middleware registration, service registration, caching, metrics, tracing, registry, validator, node catalog, auto-reconnect, graceful shutdown, hook dispatch.
**Discovered**: Multiple audits across sprints
**Fix**: Extract MiddlewareRegistry, ServiceRegistrationFlow, PendingRequestTracker
**Effort**: 1-2 days

### 7. ADRs stored in parent repository

**File**: `.forgeplan/adrs/` in parent repo (not in `moleculerpy/` git)
**Description**: PR reviewers don't see ADR changes in moleculerpy PRs. Architectural context lost.
**Fix**: Move ADRs to `moleculerpy/docs/adrs/` and include in PRs
**Effort**: ~1h

### 8. `stopped` alias uses inspect.signature — clever hack

**File**: `moleculerpy/broker.py` — `_alias_args()` helper
**Description**: Works via runtime introspection to handle legacy `Middleware.stopped()` 0-arg vs Node.js `stopped(broker)`. This is a workaround for API naming collision.
**Proper fix**: Rename legacy `Middleware.stopped()` → `Middleware.middleware_stopped()` (breaking change, needs migration for 3 existing middlewares)
**Effort**: ~2h migration + deprecation cycle

### 9. `_is_tracking_enabled` uses isinstance(dict) + getattr fallback

**File**: `moleculerpy/middleware/context_tracker.py:149-169`
**Description**: Duck typing instead of Protocol — works for both dict (legacy) and TrackingConfig (modern) but not type-safe
**Fix**: Define `TrackingConfigProtocol` and narrow via isinstance
**Effort**: ~30 min

---

## P3 — Backlog

### 10. Codecov patch coverage consistently fails

**Description**: Integration-only code paths (transporters with real services, async background tasks) aren't covered by unit tests. We ignore codecov soft-fail every PR.
**Fix**: Add unit tests with heavier mocking OR document exclusion rules in .codecov.yml
**Effort**: ~4h

### 11. Sprint retro not enforced

**Description**: No CI check that PR description has "Sprint Retro" section
**Fix**: Add pr-description-check hook / GitHub action
**Effort**: ~1h

### 12. Test fixture pollution — mock_node_catalog

**File**: `tests/unit/broker_test.py` fixture
**Description**: `mock_node_catalog.local_node` setup evolved across sprints, caused regressions
**Fix**: Isolate fixtures per test class, avoid module-level fixtures for complex mocks
**Effort**: ~1h

### 13. Dynamic register() idempotency not tested

**Description**: No test verifying that calling `broker.register(same_service)` twice is safe
**Fix**: Add idempotency test
**Effort**: ~15 min

### 14. `$shutdownTimeout` camelCase alias not tested

**File**: `moleculerpy/middleware/context_tracker.py:341-344`
**Description**: Fix added for Node.js compat but no regression test
**Fix**: Add test with camelCase settings key
**Effort**: ~10 min

### 15. ContextTracker double-registration guard not tested

**File**: `moleculerpy/broker.py:143-153`
**Description**: Guard added but no regression test
**Fix**: Add test: set tracking=True AND pass ContextTrackerMiddleware in middlewares list, verify only 1 instance
**Effort**: ~10 min

---

## Closed (historical)

_Items here are kept for context. Once a release is cut, move closed items to CHANGELOG references._

- ✅ Kafka 2-node discovery broken → Fixed in v0.14.20 (PR #36)
- ✅ checkRemoteNodes/checkOfflineNodes missing → Fixed in v0.14.20 (PR #37)
- ✅ DRY transporters (~250 lines duplicated) → Fixed in v0.14.20 (PR #38)
- ✅ Transit SRP: Discovery in wrong place → Fixed in v0.14.20 (PR #39)
- ✅ seq/instanceID heartbeat checks → Fixed in v0.14.21 (PR #41)
- ✅ Redis cacher no lifecycle → Fixed in v0.14.21 (PR #42)
- ✅ Broker hook Node.js compat → Fixed in v0.14.22 (PR #44)
- ✅ Connection drain on stop → Fixed in v0.14.22 (PR #44)
- ✅ Protocol parity heartbeat {cpu} → Fixed in v0.14.22 (PR #45)
- ✅ service_starting never dispatched (dead code) → Fixed in v0.14.22 (PR #45 audit fix)
