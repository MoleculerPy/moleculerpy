# MoleculerPy - AI Agent Context

> This file provides context for AI assistants (Claude, Codex, Gemini, Copilot, etc.) working with this codebase.
>
> **Aliases**: [CLAUDE.md](./CLAUDE.md) | [CODEX.md](./CODEX.md) | [GEMINI.md](./GEMINI.md)

## Project Overview

**MoleculerPy** is a Python port of [Moleculer.js](https://moleculer.services) - a fast, modern microservices framework. It provides service discovery, load balancing, fault tolerance, and inter-service communication.

- **Version**: 0.14.1 (Protocol v4, compatible with Moleculer.js 0.14.x)
- **Python**: 3.12+
- **License**: MIT
- **Original**: Based on [pylecular](https://github.com/alvaroinckot/pylecular) by Alvaro Inckot

## Repository Structure

```
moleculerpy/
├── moleculerpy/           # Main Python package (source code)
│   ├── __init__.py        # Public API exports
│   ├── broker.py          # ServiceBroker - core orchestrator
│   ├── service.py         # Service base class
│   ├── context.py         # Request context (params, meta, caller)
│   ├── decorators.py      # @action, @event decorators
│   ├── settings.py        # Configuration management
│   ├── registry.py        # Service registry & discovery
│   ├── transit.py         # Message transport layer
│   ├── node.py            # Node representation
│   ├── errors.py          # Exception hierarchy
│   ├── cli.py             # CLI entry point
│   │
│   ├── transporter/       # Message transporters
│   │   ├── base.py        # Abstract transporter
│   │   ├── memory.py      # In-memory (single process)
│   │   ├── nats.py        # NATS transporter
│   │   └── redis.py       # Redis Pub/Sub transporter
│   │
│   ├── middleware/        # Built-in middlewares (19)
│   │   ├── base.py        # Middleware base class
│   │   ├── circuit_breaker.py
│   │   ├── bulkhead.py
│   │   ├── retry.py
│   │   ├── timeout.py
│   │   ├── fallback.py
│   │   ├── caching.py
│   │   ├── tracing.py
│   │   ├── metrics.py
│   │   └── ...
│   │
│   ├── strategy/          # Load balancing strategies
│   │   ├── base.py
│   │   ├── round_robin.py
│   │   ├── random.py
│   │   ├── cpu_usage.py
│   │   ├── latency.py
│   │   └── shard.py
│   │
│   └── cacher/            # Caching backends
│       ├── base.py
│       ├── memory.py
│       └── redis.py
│
├── tests/                 # Test suite (1500+ tests)
│   ├── unit/              # Unit tests
│   ├── integration/       # Integration tests
│   ├── e2e/               # End-to-end tests
│   ├── stress/            # Performance/stress tests
│   └── services/          # Test service fixtures
│
├── examples/              # Usage examples
├── docs/                  # Documentation
│
├── pyproject.toml         # Package configuration (hatchling)
├── README.md              # Project documentation
├── QUICKSTART.md          # Getting started guide
├── CONTRIBUTING.md        # Contribution guidelines
├── CHANGELOG.md           # Version history
├── CREDITS.md             # Attributions
└── LICENSE                # MIT License
```

## Core Concepts

### ServiceBroker
The central orchestrator that manages services, handles communication, and coordinates the microservice ecosystem.

```python
from moleculerpy import ServiceBroker, Settings

settings = Settings(transporter="nats://localhost:4222")
broker = ServiceBroker(id="my-node", settings=settings)
await broker.start()
```

### Service
A logical unit containing actions (callable methods) and event handlers.

```python
from moleculerpy import Service, action, event, Context

class UserService(Service):
    name = "users"
    version = 1  # Optional: creates v1.users.* actions

    @action(params={"id": "string"})
    async def get(self, ctx: Context):
        return {"id": ctx.params["id"], "name": "John"}

    @event(name="user.created")
    async def on_user_created(self, ctx: Context):
        print(f"User created: {ctx.params}")
```

### Context
Carries request data through the call chain: params, meta, caller info, tracing spans.

```python
@action()
async def process(self, ctx: Context):
    # Access params
    user_id = ctx.params.get("id")

    # Access meta (headers)
    auth_token = ctx.meta.get("authorization")

    # Call another service
    result = await ctx.call("other.action", {"data": "value"})

    # Emit event
    await ctx.emit("document.processed", {"id": doc_id})
```

## Key Files to Understand

| File | Purpose |
|------|---------|
| `broker.py` | ServiceBroker implementation - start here |
| `service.py` | Service base class, action/event registration |
| `context.py` | Context object, call chain, meta propagation |
| `transit.py` | Inter-node communication, message serialization |
| `registry.py` | Service discovery, endpoint management |
| `settings.py` | Configuration schema and validation |

## Architecture Patterns

### Request Flow
```
Client → broker.call() → Registry (find endpoint) → Transit (serialize)
      → Transporter (send) → [NETWORK] → Remote Node → Service.action()
```

### Event Flow
```
Service → ctx.emit() → Local Bus → Transit → Transporter → All Subscribers
```

### Middleware Chain
```
Request → Middleware1 → Middleware2 → ... → Handler → Response
                ↑                              ↓
                └──────── (reverse order) ─────┘
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific category
pytest tests/unit/ -v
pytest tests/integration/ -v
pytest tests/stress/ -v

# Run with coverage
pytest tests/ --cov=moleculerpy --cov-report=html
```

## Common Tasks

### Adding a New Middleware
1. Create file in `moleculerpy/middleware/`
2. Inherit from `BaseMiddleware`
3. Implement `local_action()` and/or `remote_action()` hooks
4. Export in `middleware/__init__.py`

### Adding a New Transporter
1. Create file in `moleculerpy/transporter/`
2. Inherit from `BaseTransporter`
3. Implement `connect()`, `disconnect()`, `subscribe()`, `publish()`
4. Export in `transporter/__init__.py`

### Adding a New Strategy
1. Create file in `moleculerpy/strategy/`
2. Inherit from `BaseStrategy`
3. Implement `select()` method
4. Export in `strategy/__init__.py`

## Related Projects

| Project | Description | Location |
|---------|-------------|----------|
| moleculerpy-repl | Interactive REPL for debugging | [GitHub](https://github.com/MoleculerPy/moleculerpy-repl) |
| moleculerpy-channels | Redis Streams channels adapter | [GitHub](https://github.com/MoleculerPy/moleculerpy-channels) |
| moleculerjs | Original Node.js implementation | [GitHub](https://github.com/moleculerjs/moleculer) |

## Code Style

- **Async/await**: All I/O operations are async
- **Type hints**: Use throughout, especially in public API
- **Docstrings**: Google style docstrings
- **Tests**: Every feature needs unit + integration tests

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `REDIS_URL` | `redis://localhost:6379` | Redis server URL |
| `MOLECULER_NAMESPACE` | `""` | Namespace for isolation |
| `MOLECULER_NODE_ID` | auto-generated | Node identifier |
| `MOLECULER_LOG_LEVEL` | `info` | Logging level |

## Performance Notes

- **Throughput**: 127K calls/sec at 10K load, 59K at 1M load
- **Latency**: 0.008ms avg at 10K, 0.017ms at 1M
- **Batching**: Uses 50K-100K batches for 500K+ concurrent calls
- See `docs/STRESS-TEST-RESULTS.md` for full benchmarks

## Links

- **Website**: https://moleculerpy.services
- **Docs**: https://moleculerpy.services/docs
- **GitHub**: https://github.com/MoleculerPy/moleculerpy
- **PyPI**: https://pypi.org/project/moleculerpy/
- **Moleculer.js**: https://moleculer.services
