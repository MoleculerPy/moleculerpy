# MoleculerPy Quick Start

Integrate Python services with Node.js Moleculer via NATS.

## Requirements

- Python 3.12+
- Docker (for NATS)
- Node.js 22+ (optional, for integration with Node.js services)

## 1. Installation

```bash
# Install moleculerpy
pip install moleculerpy

# Or for development
git clone https://github.com/MoleculerPy/moleculerpy.git
cd moleculerpy
pip install -e .
```

## 2. Start NATS

```bash
# Start NATS with JetStream
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:latest -js

# Verify
curl http://localhost:8222/healthz
# {"status":"ok"}
```

## 3. Start MoleculerPy

```bash
# From services directory
moleculerpy services/

# Or run an example
python examples/gerts_test_service.py
```

Output:
```
[MoleculerPy] Service started! Available actions:
  - v1.py-test.ping
  - v1.py-test.echo
  - v1.py-test.process
```

## 4. Testing

### Via Python

```python
import asyncio
from moleculerpy import ServiceBroker

async def main():
    broker = ServiceBroker(transporter="nats://localhost:4222")
    await broker.start()

    # Call action
    result = await broker.call("v1.py-test.ping", {})
    print(result)  # { pong: true, timestamp: '...', node: 'moleculerpy' }

    await broker.stop()

asyncio.run(main())
```

### Via Node.js REPL

```javascript
// Ping Python service
await broker.call('v1.py-test.ping', {})
// → { pong: true, timestamp: '...', node: 'moleculerpy' }

// Echo data
await broker.call('v1.py-test.echo', { data: { hello: 'world' } })
// → { echoed: { hello: 'world' }, type: 'dict' }
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server |
| `MOLECULER_NAMESPACE` | - | Namespace for isolation |
| `MOLECULER_NODE_ID` | auto | Node ID |

Example:
```bash
NATS_URL=nats://192.168.1.100:4222 \
MOLECULER_NAMESPACE=production \
moleculerpy services/
```

## Creating Your Service

```python
from moleculerpy import Service, Context, action, event, ServiceBroker, Settings
import asyncio

class MyService(Service):
    name = "my-service"

    @action(params={"query": "string"})
    async def search(self, ctx: Context):
        query = ctx.params.get("query")
        return {"results": []}

    @action()
    async def call_other(self, ctx: Context):
        # Call another service
        result = await ctx.call("other.action", {})
        return result

    @event(name="document.created")
    async def on_document(self, ctx: Context):
        print(f"Event: {ctx.params}")

async def main():
    settings = Settings(transporter="nats://localhost:4222")
    broker = ServiceBroker("my-node", settings=settings)
    await broker.register(MyService())
    await broker.start()
    await broker.wait_for_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

## Troubleshooting

### NATS connection refused

```bash
# Check if NATS is running
docker ps | grep nats

# Start if not running
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:latest -js
```

### Service not found

1. Check namespace — must match on both sides
2. Wait 2-3 seconds after startup for service discovery
3. Verify both services are connected to NATS

### Python version error

```bash
# MoleculerPy requires Python 3.12+
python --version

# If you need to upgrade
brew install python@3.12  # macOS
# or
sudo apt install python3.12  # Ubuntu
```

## Useful Links

- [MoleculerPy README](./README.md)
- [MoleculerPy Documentation](https://moleculerpy.services/docs)
- [NATS monitoring](http://localhost:8222/)
- [Moleculer.js Documentation](https://moleculer.services/docs)
