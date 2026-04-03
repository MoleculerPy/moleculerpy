# MoleculerPy

[![CI](https://github.com/MoleculerPy/moleculerpy/workflows/CI/badge.svg)](https://github.com/MoleculerPy/moleculerpy/actions)
[![PyPI version](https://img.shields.io/pypi/v/moleculerpy.svg)](https://pypi.org/project/moleculerpy/)
[![Python versions](https://img.shields.io/pypi/pyversions/moleculerpy.svg)](https://pypi.org/project/moleculerpy/)
[![codecov](https://codecov.io/gh/MoleculerPy/moleculerpy/graph/badge.svg)](https://codecov.io/gh/MoleculerPy/moleculerpy)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **🌐 Language / Язык**: [English](README.md) | [Русский](README.ru.md)

MoleculerPy is a fast, modern and powerful microservices framework for [Python](https://python.org). It helps you to build efficient, reliable & scalable services. MoleculerPy provides many features for building and managing your microservices.

**Website**: [https://moleculerpy.services](https://moleculerpy.services)

**Documentation**: [https://moleculerpy.services/docs](https://moleculerpy.services/docs)

# What's included

- Async/await native (built on asyncio)
- Request-reply concept
- Support event driven architecture with balancing
- Built-in service registry & dynamic service discovery
- Load balanced requests & events (round-robin, random, cpu-usage, latency, sharding)
- Many fault tolerance features (Circuit Breaker, Bulkhead, Retry, Timeout, Fallback)
- Plugin/middleware system (19 built-in middlewares)
- Support [Streams](https://docs.python.org/3/library/asyncio-stream.html) for large data transfer
- Service mixins
- Built-in caching solution (Memory, Redis)
- Pluggable loggers (structlog)
- Pluggable transporters (NATS, Redis, Memory)
- Pluggable parameter validator
- Multiple services on a node/server
- Master-less architecture, all nodes are equal
- Built-in metrics feature with Prometheus exporter
- Built-in tracing feature with Console exporter
- Official [REPL](https://github.com/MoleculerPy/moleculerpy-repl) and [Channels](https://github.com/MoleculerPy/moleculerpy-channels) modules

# Installation

```bash
pip install moleculerpy
```

With optional features:
```bash
# With Redis transporter
pip install moleculerpy[redis]

# With metrics & tracing
pip install moleculerpy[metrics,tracing]

# All features
pip install moleculerpy[all]
```

# Create your first microservice

This example shows you how to create a small service with an `add` action which can add two numbers and how to call it.

```python
import asyncio
from moleculerpy import ServiceBroker, Service, action, Context

# Define a service
class MathService(Service):
    name = "math"

    @action
    async def add(self, ctx: Context):
        return ctx.params["a"] + ctx.params["b"]

async def main():
    # Create a broker
    broker = ServiceBroker()

    # Register the service
    await broker.register(MathService())

    # Start the broker
    await broker.start()

    # Call service
    result = await broker.call("math.add", {"a": 5, "b": 3})
    print(f"5 + 3 = {result}")

    # Stop broker
    await broker.stop()

asyncio.run(main())
```

# Command Line Interface

MoleculerPy includes a CLI that allows you to easily start a broker and load services:

```bash
moleculerpy <service_directory> [options]
```

## CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `service_directory` | Path to directory containing service files | - |
| `--broker-id, -b` | Broker ID | `node-<current_dir_name>` |
| `--transporter, -t` | Transporter URL | `nats://localhost:4222` |
| `--log-level, -l` | Log level | `INFO` |
| `--log-format, -f` | Log format (PLAIN, JSON) | `PLAIN` |
| `--namespace, -n` | Service namespace | `default` |

## Example

```bash
# Start with services from the 'services' directory
moleculerpy services

# Custom broker ID and transporter
moleculerpy services -b my-broker -t nats://nats-server:4222

# Verbose logging
moleculerpy services -l DEBUG
```

# Official modules

We have official modules for MoleculerPy:

| Module | Description |
|--------|-------------|
| [moleculerpy-repl](https://github.com/MoleculerPy/moleculerpy-repl) | Interactive CLI shell for debugging and managing services |
| [moleculerpy-channels](https://github.com/MoleculerPy/moleculerpy-channels) | Reliable pub/sub messaging with Redis, Kafka, NATS |

# Middlewares

MoleculerPy provides a powerful middleware system to extend functionality. Middlewares can hook into various stages of request, event, and lifecycle processes.

```python
from moleculerpy.middleware import Middleware
from moleculerpy.context import Context

class LoggingMiddleware(Middleware):
    async def local_action(self, next_handler, action_endpoint):
        async def wrapped_handler(ctx: Context):
            print(f"Before action: {action_endpoint.name}")
            result = await next_handler(ctx)
            print(f"After action: {action_endpoint.name}")
            return result
        return wrapped_handler

# Register middleware
broker = ServiceBroker(middlewares=[LoggingMiddleware()])
```

## Available Hooks

### Wrapping Hooks
- `local_action(next_handler, action_endpoint)` - Wraps local action handlers
- `remote_action(next_handler, action_endpoint)` - Wraps remote action calls
- `local_event(next_handler, event_endpoint)` - Wraps local event handlers

### Lifecycle Hooks
- `broker_created(broker)` - Called after broker initialization
- `broker_started(broker)` - Called after broker startup
- `broker_stopped(broker)` - Called after broker shutdown
- `service_created(service)` - Called after service registration
- `service_started(service)` - Called after service startup

# Roadmap

## Current status (v0.14.7)
- Core framework with full service lifecycle
- **Service versioning** — `v1.users.get`, `v2.users.get` coexistence, `$noVersionPrefix`
- NATS, Redis, Memory transporters
- Pluggable serializers (JSON, MsgPack) — MsgPack 2x faster on large payloads
- Balanced request/event handling via NATS queue groups (`disable_balancer=True`)
- 4/4 transit middleware hooks (publish, send, receive, message handler)
- 22 built-in middlewares
- 6 load balancing strategies (including Shard)
- Circuit breaker, bulkhead, retry patterns
- Prometheus metrics & Console tracing
- Streaming support
- Protocol v4 safety: version check, NodeID conflict detection
- REPL & Channels modules
- Pre-commit/push hooks (ruff, mypy, pytest) + Codecov integration

## Planned features
- REST API Gateway (moleculerpy-web)
- TCP transporter with Gossip protocol
- Database adapters (moleculerpy-db)
- LRU cache
- Jaeger & Zipkin tracing exporters
- Service versioning (v1.users.get action naming)
- Kafka transporter
- AMQP transporter
- GraphQL gateway
- gRPC support

# Documentation

You can find the documentation at [https://moleculerpy.services/docs](https://moleculerpy.services/docs).

# Changelog

See [CHANGELOG.md](CHANGELOG.md).

# Contributing

We welcome you to join in the development of MoleculerPy. Please read our [contribution guide](CONTRIBUTING.md).

# Credits

This project is based on [pylecular](https://github.com/alvaroinckot/pylecular) by Alvaro Inckot, licensed under MIT License.

Inspired by [Moleculer.js](https://moleculer.services) - the original Node.js microservices framework.

# License

MoleculerPy is available under the [MIT license](LICENSE).

# Contact

Copyright (c) 2026 MoleculerPy

[![@MoleculerPy](https://img.shields.io/badge/github-MoleculerPy-green.svg)](https://github.com/MoleculerPy)
