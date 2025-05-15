# MoleculerPy Cluster Demo

Interactive multi-node cluster demo with resilience middleware and load testing.

## Prerequisites

1. **NATS Server** running on `localhost:4222`

```bash
# Docker
docker run -d --name nats -p 4222:4222 nats:latest

# Or install locally
# macOS: brew install nats-server && nats-server
# Linux: See https://docs.nats.io/running-a-nats-service/introduction/installation
```

## Quick Start

### Terminal 1: Start Server (Beta Node)

```bash
cd /Users/explosovebit/Work/GertsAi/gertsai_codex/sources/moleculerpy
.venv/bin/python examples/cluster/server_beta.py
```

### Terminal 2: Start Client (Alpha Node)

```bash
cd /Users/explosovebit/Work/GertsAi/gertsai_codex/sources/moleculerpy
.venv/bin/python examples/cluster/client_alpha.py
```

### Terminal 3 (Optional): Second Server for Load Balancing

```bash
cd /Users/explosovebit/Work/GertsAi/gertsai_codex/sources/moleculerpy
.venv/bin/python examples/cluster/server_gamma.py
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        NATS Server                          │
│                     localhost:4222                          │
└───────────────┬─────────────────────────┬───────────────────┘
                │                         │
        ┌───────┴───────┐         ┌───────┴───────┐
        │  alpha-node   │         │  beta-node    │
        │   (client)    │         │   (server)    │
        │               │   call  │               │
        │  Middleware:  │ ──────► │  Services:    │
        │  - Retry      │         │  - flaky.*    │
        │  - Circuit    │         │  - slow.*     │
        │  - Timeout    │         │  - counter.*  │
        │  - Bulkhead   │         │  - echo.*     │
        └───────────────┘         └───────────────┘
                                          │
                                  ┌───────┴───────┐
                                  │  gamma-node   │
                                  │   (server)    │
                                  │  Same services│
                                  │  (load bal.)  │
                                  └───────────────┘
```

## Interactive Menu

The client provides an interactive menu:

```
  1. Test echo.ping (simple call)
  2. Test retry (flaky.unreliable)
  3. Test circuit breaker (flaky.always_fail)
  4. Test timeout (slow.process)
  5. Test counter (load balancing)

  Load Tests:
  6. Load test echo (100 req, 10 concurrent)
  7. Load test flaky (100 req, 10 concurrent)
  8. Load test counter (500 req, 50 concurrent)
  9. Custom load test
```

## Middleware Stack (Client)

| Middleware | Configuration | Purpose |
|------------|---------------|---------|
| **RetryMiddleware** | 3 retries, 0.1s base delay | Auto-retry on transient failures |
| **CircuitBreakerMiddleware** | 50% threshold, 5 min requests | Fail-fast when service is down |
| **TimeoutMiddleware** | 2s default timeout | Prevent hanging requests |
| **BulkheadMiddleware** | 20 concurrent, 100 queue | Limit concurrent requests |

## Services (Server)

| Service | Action | Behavior |
|---------|--------|----------|
| `flaky` | `unreliable` | Fails ~40% of the time (tests retry) |
| `flaky` | `always_fail` | Always fails (tests circuit breaker) |
| `slow` | `process` | Configurable delay (tests timeout) |
| `counter` | `increment` | Counts calls (tests load balancing) |
| `echo` | `ping` | Simple echo (basic connectivity) |

## Load Test Results Example

```
╔════════════════════════════════════════════════════════╗
║                   LOAD TEST RESULTS                     ║
╠════════════════════════════════════════════════════════╣
║  Total Requests:          100                           ║
║  Successes:                95  (95.0%)                  ║
║  Failures:                  5                           ║
║    - Timeouts:              0                           ║
║    - Circuit Breaks:        0                           ║
╠════════════════════════════════════════════════════════╣
║  Response Times (ms):                                   ║
║    - Average:            12.34                          ║
║    - P50 (Median):        8.50                          ║
║    - P95:                45.20                          ║
║    - P99:                98.30                          ║
║    - Min:                 2.10                          ║
║    - Max:               156.80                          ║
╚════════════════════════════════════════════════════════╝
```

## Tips

1. **Watch server logs** - Each server prints detailed logs of incoming calls
2. **Add gamma-node** - Launch `server_gamma.py` to see load balancing in action
3. **Test circuit breaker** - Run option 3 multiple times to see circuit trip
4. **Custom load test** - Option 9 lets you configure requests/concurrency
