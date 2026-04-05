"""Kafka integration demo — L2/L3 evidence for v0.14.16.

Requires: Docker Kafka on localhost:9092
"""

import asyncio
import json
import time
import uuid

import pytest

try:
    from aiokafka import AIOKafkaProducer

    HAS_AIOKAFKA = True
except ImportError:
    HAS_AIOKAFKA = False

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings

KAFKA_URL = "localhost:9092"


async def _check_kafka() -> bool:
    if not HAS_AIOKAFKA:
        return False
    try:
        p = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)
        await p.start()
        await p.stop()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def kafka_available():
    available = asyncio.get_event_loop().run_until_complete(_check_kafka())
    if not available:
        pytest.skip("Kafka not reachable at localhost:9092")


class GreeterService(Service):
    name = "greeter"

    def __init__(self):
        super().__init__(self.name)

    @action()
    async def hello(self, ctx):
        return f"Hello, {ctx.params.get('name', 'World')}!"

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


class TestKafkaIntegration:
    """Integration tests with real Kafka."""

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self, kafka_available):
        """CHECK 1: Connect/disconnect to Kafka."""
        from unittest.mock import Mock

        from moleculerpy.serializers import JsonSerializer
        from moleculerpy.transporter.kafka import KafkaTransporter

        transit = Mock()
        transit.serializer = JsonSerializer()

        t = KafkaTransporter(
            bootstrap_servers=KAFKA_URL,
            transit=transit,
            handler=None,
            node_id="kafka-test-1",
        )
        await t.connect()
        assert t._producer is not None
        await t.disconnect()
        assert t._producer is None
        print("CHECK 1 PASSED: Connect/disconnect to Kafka")

    @pytest.mark.asyncio
    async def test_produce_and_consume(self, kafka_available):
        """CHECK 2: Produce and consume a message via Kafka."""
        from unittest.mock import AsyncMock, Mock

        from moleculerpy.serializers import JsonSerializer
        from moleculerpy.transporter.kafka import KafkaTransporter

        received = asyncio.Event()
        received_data = {}

        async def handler(packet):
            received_data["packet"] = packet
            received.set()

        transit = Mock()
        transit.serializer = JsonSerializer()

        t = KafkaTransporter(
            bootstrap_servers=KAFKA_URL,
            transit=transit,
            handler=handler,
            node_id="kafka-consumer-1",
            group_id=f"test-{uuid.uuid4().hex[:6]}",
        )
        await t.connect()

        # Subscribe via make_subscriptions
        await t.make_subscriptions([{"cmd": "HEARTBEAT"}])

        await asyncio.sleep(2.0)

        # Produce via the transporter's producer
        payload = json.dumps({"sender": "kafka-sender", "cpu": 42}).encode()
        await t._producer.send_and_wait("MOL.HEARTBEAT", value=payload)

        try:
            await asyncio.wait_for(received.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail("Did not receive Kafka message within 10 seconds")

        assert received_data["packet"].sender == "kafka-sender"
        print("CHECK 2 PASSED: Produce and consume via Kafka")

        await t.disconnect()


class TestKafkaFullDemo:
    """Full demo with real Kafka broker."""

    @pytest.mark.asyncio
    async def test_single_node_local_call(self, kafka_available):
        """CHECK 3: Single node with Kafka transport."""
        suffix = uuid.uuid4().hex[:6]
        settings = Settings(transporter=f"kafka://{KAFKA_URL}", log_level="ERROR")
        broker = ServiceBroker(id=f"kafka-loc-{suffix}", settings=settings)
        await broker.register(GreeterService())

        await broker.start()
        try:
            result = await broker.call("greeter.hello", {"name": "Kafka"})
            assert result == "Hello, Kafka!"
            print("CHECK 3 PASSED: Local call via Kafka node")
        finally:
            await broker.stop()

    @pytest.mark.asyncio
    async def test_local_call_benchmark(self, kafka_available):
        """CHECK 4: Benchmark — local calls on Kafka node."""
        suffix = uuid.uuid4().hex[:6]
        settings = Settings(transporter=f"kafka://{KAFKA_URL}", log_level="ERROR")
        broker = ServiceBroker(id=f"kafka-bench-{suffix}", settings=settings)
        await broker.register(GreeterService())

        await broker.start()
        try:
            for _ in range(50):
                await broker.call("greeter.add", {"a": 1, "b": 2})

            n = 1000
            start = time.perf_counter()
            for _ in range(n):
                await broker.call("greeter.add", {"a": 1, "b": 2})
            elapsed = time.perf_counter() - start
            rps = n / elapsed
            latency_ms = (elapsed / n) * 1000

            print(f"CHECK 4 PASSED: {rps:.0f} req/sec, {latency_ms:.3f}ms latency")
            assert rps > 100
        finally:
            await broker.stop()

    @pytest.mark.asyncio
    async def test_registry_resolves_kafka(self, kafka_available):
        """CHECK 5: Registry resolves 'kafka'."""
        from unittest.mock import AsyncMock, Mock

        from moleculerpy.serializers import JsonSerializer
        from moleculerpy.transporter.base import Transporter
        from moleculerpy.transporter.kafka import KafkaTransporter

        transit = Mock()
        transit.serializer = JsonSerializer()

        t = Transporter.get_by_name(
            "kafka", {"connection": f"kafka://{KAFKA_URL}"}, transit, AsyncMock(), "reg-test"
        )
        assert isinstance(t, KafkaTransporter)
        await t.connect()
        assert t._producer is not None
        await t.disconnect()
        print("CHECK 5 PASSED: Registry resolves 'kafka'")
