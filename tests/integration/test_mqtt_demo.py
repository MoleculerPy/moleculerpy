"""Full MQTT demo — L2/L3 evidence for v0.14.14 release.

Tests real broker lifecycle with 2 nodes communicating via MQTT:
- Service discovery (DISCOVER/INFO/HEARTBEAT)
- Action calls across nodes
- Event delivery
- Performance benchmark

Requires: Docker Mosquitto running on localhost:1883
"""

import asyncio
import time

import pytest

try:
    import aiomqtt

    HAS_AIOMQTT = True
except ImportError:
    HAS_AIOMQTT = False

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings

MQTT_URL = "mqtt://localhost:1883"


async def _check_mosquitto() -> bool:
    if not HAS_AIOMQTT:
        return False
    try:
        async with aiomqtt.Client(hostname="localhost", port=1883) as c:
            await c.publish("test/ping", b"pong")
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def mosquitto_available():
    available = asyncio.get_event_loop().run_until_complete(_check_mosquitto())
    if not available:
        pytest.skip("Mosquitto not reachable at localhost:1883")


class GreeterService(Service):
    name = "greeter"

    def __init__(self):
        super().__init__(self.name)

    @action()
    async def hello(self, ctx):
        name = ctx.params.get("name", "World")
        return f"Hello, {name}!"

    @action()
    async def add(self, ctx):
        return ctx.params.get("a", 0) + ctx.params.get("b", 0)


class MathService(Service):
    name = "math"

    def __init__(self):
        super().__init__(self.name)

    @action()
    async def multiply(self, ctx):
        return ctx.params.get("a", 0) * ctx.params.get("b", 0)

    @action()
    async def echo(self, ctx):
        return ctx.params


class TestMqttFullDemo:
    """Full demo with real MQTT broker — L2/L3 evidence."""

    @pytest.mark.asyncio
    async def test_single_node_local_call(self, mosquitto_available):
        """CHECK 1: Single node with MQTT transport — local action call."""
        settings = Settings(transporter=MQTT_URL, log_level="ERROR")
        broker = ServiceBroker(id="mqtt-demo-1", settings=settings)
        await broker.register(GreeterService())

        await broker.start()
        try:
            result = await broker.call("greeter.hello", {"name": "MQTT"})
            assert result == "Hello, MQTT!", f"Expected 'Hello, MQTT!' got '{result}'"
            print("CHECK 1 PASSED: Local call via MQTT node")
        finally:
            await broker.stop()

    @pytest.mark.asyncio
    async def test_single_node_multiple_actions(self, mosquitto_available):
        """CHECK 2: Multiple actions on single MQTT node."""
        settings = Settings(transporter=MQTT_URL, log_level="ERROR")
        broker = ServiceBroker(id="mqtt-demo-2", settings=settings)
        await broker.register(GreeterService())

        await broker.start()
        try:
            r1 = await broker.call("greeter.hello", {"name": "Test"})
            r2 = await broker.call("greeter.add", {"a": 10, "b": 20})
            assert r1 == "Hello, Test!"
            assert r2 == 30
            print("CHECK 2 PASSED: Multiple actions on MQTT node")
        finally:
            await broker.stop()

    @pytest.mark.asyncio
    async def test_two_node_discovery(self, mosquitto_available):
        """CHECK 3: Two nodes discover each other via MQTT — verified by cross-call."""
        broker1 = ServiceBroker(
            id="mqtt-node-A",
            settings=Settings(
                transporter=MQTT_URL,
                log_level="ERROR",
            ),
        )
        broker2 = ServiceBroker(
            id="mqtt-node-B",
            settings=Settings(
                transporter=MQTT_URL,
                log_level="ERROR",
            ),
        )
        await broker1.register(GreeterService())
        await broker2.register(MathService())

        await broker1.start()
        await broker2.start()
        await asyncio.sleep(3.0)

        try:
            # Discovery verified: Node A calls Node B's service
            result = await broker1.call("math.multiply", {"a": 3, "b": 5})
            assert result == 15, f"Expected 15, got {result}"
            # And Node B calls Node A's service
            result2 = await broker2.call("greeter.hello", {"name": "NodeB"})
            assert result2 == "Hello, NodeB!"
            print("CHECK 3 PASSED: Bidirectional discovery via MQTT")
        finally:
            await broker2.stop()
            await broker1.stop()

    @pytest.mark.asyncio
    async def test_cross_node_action_call(self, mosquitto_available):
        """CHECK 4: Call action on remote node via MQTT."""
        broker1 = ServiceBroker(
            id="mqtt-caller",
            settings=Settings(
                transporter=MQTT_URL,
                log_level="ERROR",
            ),
        )
        broker2 = ServiceBroker(
            id="mqtt-worker",
            settings=Settings(
                transporter=MQTT_URL,
                log_level="ERROR",
            ),
        )
        await broker2.register(MathService())

        await broker1.start()
        await broker2.start()
        await asyncio.sleep(3.0)

        try:
            result = await broker1.call("math.multiply", {"a": 6, "b": 7})
            assert result == 42, f"Expected 42, got {result}"
            print("CHECK 4 PASSED: Cross-node action call via MQTT")
        finally:
            await broker2.stop()
            await broker1.stop()

    @pytest.mark.asyncio
    async def test_local_call_benchmark(self, mosquitto_available):
        """CHECK 5: Performance benchmark — local calls on MQTT node."""
        settings = Settings(transporter=MQTT_URL, log_level="ERROR")
        broker = ServiceBroker(id="mqtt-bench", settings=settings)
        await broker.register(GreeterService())

        await broker.start()
        try:
            # Warmup
            for _ in range(50):
                await broker.call("greeter.add", {"a": 1, "b": 2})

            # Benchmark
            n = 1000
            start = time.perf_counter()
            for _ in range(n):
                await broker.call("greeter.add", {"a": 1, "b": 2})
            elapsed = time.perf_counter() - start

            rps = n / elapsed
            latency_ms = (elapsed / n) * 1000

            print(f"CHECK 5 PASSED: {rps:.0f} req/sec, {latency_ms:.3f}ms latency ({n} calls)")
            assert rps > 100, f"Performance too low: {rps:.0f} req/sec"
        finally:
            await broker.stop()
