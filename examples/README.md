# MoleculerPy Integration Examples

Examples of integrating Python services with Node.js Moleculer pipeline via NATS.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    NATS Server (4222)                        │
│              Topic: MOL.{COMMAND}[.{NODE_ID}]               │
└───────────────────┬─────────────────────┬───────────────────┘
                    │                     │
        ┌───────────▼───────────┐  ┌──────▼────────────┐
        │   Node.js Pipeline    │  │    MoleculerPy      │
        │   (Moleculer)         │  │    (Python)       │
        │                       │  │                   │
        │ Services:             │  │ Services:         │
        │ • v1.api.health       │  │ • v1.py-test      │
        │ • v1.llm.chat         │  │   - ping          │
        │ • v1.graph.query      │  │   - echo          │
        │ • v1.queue.*          │  │   - process       │
        └───────────────────────┘  └───────────────────┘

        ┌─────── Cross-Language RPC ──────────┐
        │ Node.js → Python: broker.call('v1.py-test.ping', {})
        │ Python → Node.js: ctx.call('v1.api.health', {})
        └─────────────────────────────────────┘
```

## Quick Start

### 1. Start NATS

```bash
# From project root
cd infra/pipeline
docker compose up nats -d

# Verify
curl http://localhost:8222/healthz
```

### 2. Start Node.js Pipeline with NATS

```bash
# From project root
TRANSPORT_TYPE=NATS pnpm --filter @gerts/pipeline dev
```

### 3. Start Python Service

```bash
# From moleculerpy directory
cd sources/moleculerpy
./examples/run.sh

# Or directly
.venv/bin/python examples/gerts_test_service.py
```

## Testing Connectivity

### From Node.js (via API or REPL)

```javascript
// Ping Python service
await broker.call('v1.py-test.ping', {});
// Response: { pong: true, timestamp: '...', node: 'moleculerpy' }

// Echo data
await broker.call('v1.py-test.echo', { data: { hello: 'world' } });
// Response: { echoed: { hello: 'world' }, type: 'dict', source: 'moleculerpy' }

// Process items
await broker.call('v1.py-test.process', { items: ['a', 'b', 'c'] });
// Response: { success: true, processed: [...], total: 3 }
```

### Via MCP/curl

```bash
# Ping Python service
curl -X POST http://localhost:3023/mcp/call \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "action": "v1.py-test.ping",
    "params": {}
  }'
```

## Configuration

| Variable | Default Value | Description |
|----------|---------------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server |
| `MOLECULER_NAMESPACE` | `graphrag` | Moleculer namespace |
| `MOLECULER_NODE_ID` | `moleculerpy-test` | Node ID for MoleculerPy |

## Creating Your Python Service

```python
from moleculerpy.service import Service
from moleculerpy.context import Context
from moleculerpy.decorators import action, event

class MyService(Service):
    name = "v1.my-service"  # Use v1. prefix for compatibility

    def __init__(self):
        super().__init__(self.name)

    @action(params={"query": "string"})
    async def search(self, ctx: Context):
        query = ctx.params.get("query")
        # Your logic here
        return {"results": [...]}

    @event(name="document.created")
    async def on_document_created(self, ctx: Context):
        print(f"New document: {ctx.params}")
```

## Troubleshooting

### NATS connection refused

```bash
# Check if NATS is running
docker ps | grep nats
docker compose -f infra/pipeline/docker-compose.yml up nats -d
```

### Service not found

1. Check that namespace matches (`MOLECULER_NAMESPACE=graphrag`)
2. Verify both services are connected to NATS
3. Wait a few seconds for service discovery

### Python version error

```bash
# MoleculerPy requires Python 3.12+
python3.12 --version

# If not installed, use brew
brew install python@3.12
```
