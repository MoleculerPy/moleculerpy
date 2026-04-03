# PRD-011: Service Versioning

**Version**: 0.14.7
**Status**: Draft
**Date**: 2026-04-03
**FPF State**: Shaping (B.5.1)
**Scale**: Standard (1-3 days)

---

## Problem

MoleculerPy services cannot declare a version. In Moleculer.js, a service with `version: 2` registers actions as `"v2.users.get"`, allowing multiple versions of the same service to coexist. MoleculerPy has a stub: `version` is read in `__init_subclass__` and stored in schema/tracing metadata, but **never used in action naming or registry routing**.

This blocks:
- Running v1 and v2 of a service simultaneously (canary, A/B, migration)
- Protocol compatibility with Node.js nodes that send versioned INFO packets
- Feature parity goal (listed in TODO as P3)

## Goals

- **G-1**: Support versioned action names (`"v2.users.get"`) identical to Node.js format
- **G-2**: Multiple versions of one service can coexist in the same broker
- **G-3**: Protocol v4 compatible — INFO packets include `version` and `fullName`
- **G-4**: Zero breaking changes — unversioned services work exactly as before

## Non-Goals

- API Gateway version routing (moleculerpy-web concern, not core)
- Automatic version negotiation ("give me latest version")
- Deprecation warnings for old versions
- Event name versioning (events don't use service prefix in Moleculer.js)

## Reference Implementation

**Node.js Moleculer** (`sources/reference-implementations/moleculer/`):

```js
// service.js:835 — name construction
static getVersionedFullName(name, version) {
    if (version != null)
        return (typeof version == "number" ? "v" + version : version) + "." + name;
    return name;
}

// service.js:110 — fullName set at construction
this.fullName = Service.getVersionedFullName(
    this.name,
    this.settings.$noVersionPrefix !== true ? this.version : undefined
);

// service.js:382 — action name = fullName + "." + rawName
action.name = this.fullName + "." + action.rawName;  // "v2.users.get"

// registry.js:198 — remote services reconstruct fullName
if (!svc.fullName)
    svc.fullName = this.broker.ServiceFactory.getVersionedFullName(svc.name, svc.version);
```

**Key insight**: version prefix is baked into `fullName` once. Registry stores by full action name — no version-aware lookup logic needed.

## Functional Requirements

### FR-001: Service.full_name property

| ID | Requirement |
|---|---|
| FR-001.1 | `Service` gains a `full_name: str` property computed from `name` + `version` |
| FR-001.2 | Numeric version `2` → prefix `"v2"` → full_name = `"v2.users"` |
| FR-001.3 | String version `"staging"` → prefix `"staging"` → full_name = `"staging.users"` |
| FR-001.4 | No version → full_name = name (backwards compatible) |
| FR-001.5 | `$noVersionPrefix: true` in settings → version ignored, full_name = name |
| FR-001.6 | Static method `get_versioned_full_name(name, version)` for reuse |

### FR-002: Registry uses full_name

| ID | Requirement |
|---|---|
| FR-002.1 | `register()` builds action name as `full_name + "." + raw_name` |
| FR-002.2 | Services keyed by `full_name` in `__services__` dict |
| FR-002.3 | `broker.call("v2.users.get")` resolves correctly |
| FR-002.4 | Two versions of same service (v1.users, v2.users) coexist |

### FR-003: Transit/discovery version support

| ID | Requirement |
|---|---|
| FR-003.1 | INFO packets include `version` and `fullName` per service |
| FR-003.2 | Remote services without `fullName` → reconstruct from name + version |
| FR-003.3 | Balanced subscriptions use full_name for topic names |

### FR-004: Settings flags

| ID | Requirement |
|---|---|
| FR-004.1 | `$noVersionPrefix: true` → suppress version prefix |
| FR-004.2 | `$noServiceNamePrefix: true` → action name = raw name only |

## Technical Design

### Changes by file

| File | Change | Lines affected |
|---|---|---|
| `service.py` | Add `version` attr, `full_name` property, `get_versioned_full_name()` static | ~30 new lines |
| `registry.py:279` | Key services by `service.full_name` instead of `service.name` | 1 line |
| `registry.py:287` | Action name: `f"{service.full_name}.{raw_name}"` | 1 line |
| `node.py:460` | `"fullName": service.full_name` + add `"version"` field | 2 lines |
| `transit.py:1116` | Use `full_name` for balanced subscriptions | 1 line |
| `protocols.py` | Ensure ServiceProtocol has version field (already exists) | verify only |
| `middleware/tracing.py` | Already reads `getattr(service, "version")` | verify only |

### Data flow

```
class UserService(Service):       Service.__init__()
    name = "users"          →     self.name = "users"
    version = 2                   self.version = 2
                                  self.full_name = "v2.users"  # NEW

Registry.register(service)  →     Action(name="v2.users.get", ...)
                                  __services__["v2.users"] = service

broker.call("v2.users.get") →     registry.get_action("v2.users.get")
                                  → direct dict lookup, no version parsing

Node.ensure_local_node()    →     {"name": "users", "version": 2, "fullName": "v2.users"}
```

## Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Breaking existing services without version | HIGH | G-4: no version → full_name = name (identity) |
| Service key collision (name vs full_name) | MEDIUM | full_name for unversioned = name, so keys identical |
| Remote Node.js node sends version we don't handle | LOW | FR-003.2: reconstruct fullName from name+version |

## Evidence Criteria (FPF Phase 3)

- [ ] Unit tests: `get_versioned_full_name()` — numeric, string, None, $noVersionPrefix
- [ ] Unit tests: Registry with versioned + unversioned services coexisting
- [ ] Unit tests: `broker.call("v2.users.get")` routes correctly
- [ ] Integration: Two versions of same service, both reachable
- [ ] Compatibility: INFO packet format matches Node.js
- [ ] Regression: All 1702 existing tests pass
- [ ] Type check: mypy strict clean
- [ ] Lint: ruff clean

## Timeline

| Day | Phase | Deliverable |
|---|---|---|
| 1 | Shape + Code | PRD + Service.full_name + Registry changes |
| 2 | Code + Evidence | Transit + Tests (20+ new) |
| 3 | Evidence + Operate | Audit + Version bump + PR + Tag |
