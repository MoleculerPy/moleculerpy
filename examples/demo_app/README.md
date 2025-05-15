# MoleculerPy Demo App

A full-featured demo application consisting of multiple services built on top of:

- `moleculerpy` for service discovery and RPC
- `moleculerpy-channels` for pub/sub via NATS JetStream
- `moleculerpy-repl` for a live console on top of a running broker

## Services

- `catalog` — product catalog
- `orders` — order creation and publishing `orders.created`
- `payments` — payment processing and publishing `payments.completed`
- `analytics` — reads channel events and collects metrics
- `notifications` — reads channel events and generates notifications
- `gateway` — orchestration action `checkout` and `dashboard` summary

## Requirements

- Python virtualenv from the repository root
- NATS with JetStream enabled on `localhost:4222`

Example Docker run:

```bash
docker run --rm -p 4222:4222 nats:latest -js
```

## Quick smoke run

A single process will start a demo broker and a client broker, then run the checkout flow:

```bash
cd /Users/explosovebit/Work/MoleculerPy
.venv/bin/python moleculerpy/examples/demo_app/smoke_run.py
```

## Separate server + client launch

Terminal 1:

```bash
cd /Users/explosovebit/Work/MoleculerPy
.venv/bin/python moleculerpy/examples/demo_app/server.py
```

Terminal 2:

```bash
cd /Users/explosovebit/Work/MoleculerPy
.venv/bin/python moleculerpy/examples/demo_app/client.py
```

## Live REPL

```bash
cd /Users/explosovebit/Work/MoleculerPy
.venv/bin/python moleculerpy/examples/demo_app/repl_console.py
```

Useful commands:

```text
info
services
actions
call catalog.list
call gateway.checkout product_id=starter-kit quantity=2 customer_name=Alice
call gateway.dashboard
```

## Launch via `moleculerpy-runner`

The demo can now also be started via the runner with a Python config file:

```bash
cd /Users/explosovebit/Work/MoleculerPy
.venv/bin/moleculerpy-runner \
  -c moleculerpy/examples/demo_app/broker_config.py \
  --repl \
  moleculerpy/examples/demo_app/services
```

If the entrypoint is not yet installed in the current `venv`, use the fallback:

```bash
cd /Users/explosovebit/Work/MoleculerPy
PYTHONPATH=moleculerpy-repl/src:moleculerpy:moleculerpy-channels \
  .venv/bin/python -m moleculerpy_repl.runner \
  -c moleculerpy/examples/demo_app/broker_config.py \
  --repl \
  moleculerpy/examples/demo_app/services
```

## Why not just `moleculerpy-runner --repl`

For `channels` a custom broker factory is needed, which is why
[broker_config.py](/Users/explosovebit/Work/MoleculerPy/moleculerpy/examples/demo_app/broker_config.py) is used.
The separate launcher scripts are still kept because they are more convenient for smoke runs and two-terminal demos.
