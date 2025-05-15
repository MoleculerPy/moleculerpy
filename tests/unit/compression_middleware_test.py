"""Unit tests for the Compression Middleware module.

Tests for CompressionMiddleware that provides transit message compression
using deflate, gzip, or zlib compression methods.
"""

import asyncio

import pytest

from moleculerpy.middleware.compression import (
    FLAG_COMPRESSED,
    FLAG_NOT_COMPRESSED,
    CompressionMethod,
    CompressionMiddleware,
)


class TestCompressionMethod:
    """Tests for CompressionMethod enum."""

    def test_deflate_value(self):
        """Test DEFLATE enum value."""
        assert CompressionMethod.DEFLATE.value == "deflate"

    def test_gzip_value(self):
        """Test GZIP enum value."""
        assert CompressionMethod.GZIP.value == "gzip"

    def test_zlib_value(self):
        """Test ZLIB enum value."""
        assert CompressionMethod.ZLIB.value == "zlib"

    def test_all_methods_count(self):
        """Test all compression methods are present."""
        assert len(CompressionMethod) == 3


class TestCompressionMiddlewareInit:
    """Tests for CompressionMiddleware initialization."""

    def test_init_default_values(self):
        """Test middleware initializes with default values."""
        mw = CompressionMiddleware()

        assert mw.method == CompressionMethod.DEFLATE
        assert mw.threshold == 1024
        assert mw.level == 6

    def test_init_with_enum_method(self):
        """Test middleware with enum method."""
        mw = CompressionMiddleware(method=CompressionMethod.GZIP)

        assert mw.method == CompressionMethod.GZIP

    def test_init_with_string_method(self):
        """Test middleware with string method (backward compat)."""
        mw = CompressionMiddleware(method="zlib")

        assert mw.method == CompressionMethod.ZLIB

    def test_init_with_custom_threshold(self):
        """Test middleware with custom threshold."""
        mw = CompressionMiddleware(threshold=512)

        assert mw.threshold == 512

    def test_init_with_custom_level(self):
        """Test middleware with custom compression level."""
        mw = CompressionMiddleware(level=9)

        assert mw.level == 9

    def test_init_invalid_string_method_raises(self):
        """Test invalid string method raises ValueError."""
        with pytest.raises(ValueError, match="Unknown compression method"):
            CompressionMiddleware(method="invalid")

    def test_init_invalid_method_shows_available(self):
        """Test error message shows available methods."""
        with pytest.raises(ValueError, match="deflate, gzip, zlib"):
            CompressionMiddleware(method="lz4")


class TestCompressionDecompression:
    """Tests for compression and decompression logic."""

    @pytest.fixture
    def middleware_deflate(self) -> CompressionMiddleware:
        """Create deflate middleware."""
        return CompressionMiddleware(method=CompressionMethod.DEFLATE)

    @pytest.fixture
    def middleware_gzip(self) -> CompressionMiddleware:
        """Create gzip middleware."""
        return CompressionMiddleware(method=CompressionMethod.GZIP)

    @pytest.fixture
    def middleware_zlib(self) -> CompressionMiddleware:
        """Create zlib middleware."""
        return CompressionMiddleware(method=CompressionMethod.ZLIB)

    def test_compress_decompress_deflate(self, middleware_deflate):
        """Test deflate compression roundtrip."""
        original = b"Hello, World! " * 100
        compressed = middleware_deflate._compress(original)
        decompressed = middleware_deflate._decompress(compressed)

        assert decompressed == original
        assert len(compressed) < len(original)

    def test_compress_decompress_gzip(self, middleware_gzip):
        """Test gzip compression roundtrip."""
        original = b"Hello, World! " * 100
        compressed = middleware_gzip._compress(original)
        decompressed = middleware_gzip._decompress(compressed)

        assert decompressed == original
        assert len(compressed) < len(original)

    def test_compress_decompress_zlib(self, middleware_zlib):
        """Test zlib compression roundtrip."""
        original = b"Hello, World! " * 100
        compressed = middleware_zlib._compress(original)
        decompressed = middleware_zlib._decompress(compressed)

        assert decompressed == original
        assert len(compressed) < len(original)

    def test_empty_data_compression(self, middleware_deflate):
        """Test compressing empty data."""
        original = b""
        compressed = middleware_deflate._compress(original)
        decompressed = middleware_deflate._decompress(compressed)

        assert decompressed == original

    def test_binary_data_compression(self, middleware_deflate):
        """Test compressing binary data."""
        original = bytes(range(256)) * 10
        compressed = middleware_deflate._compress(original)
        decompressed = middleware_deflate._decompress(compressed)

        assert decompressed == original

    def test_incompressible_data(self, middleware_deflate):
        """Test with random (incompressible) data."""
        import secrets

        original = secrets.token_bytes(1000)
        compressed = middleware_deflate._compress(original)
        decompressed = middleware_deflate._decompress(compressed)

        assert decompressed == original
        # Compressed may be larger for random data

    def test_decompress_invalid_data_raises(self, middleware_deflate):
        """Test decompressing invalid data raises error."""
        import zlib

        with pytest.raises(zlib.error):
            middleware_deflate._decompress(b"invalid compressed data")


class TestTransporterSendHook:
    """Tests for transporter_send hook."""

    @pytest.fixture
    def middleware(self) -> CompressionMiddleware:
        """Create middleware with low threshold for testing."""
        return CompressionMiddleware(threshold=10)

    @pytest.mark.asyncio
    async def test_compress_above_threshold(self, middleware):
        """Test data above threshold is compressed."""
        captured_data: list[bytes] = []

        async def mock_send(topic: str, data: bytes, meta: dict) -> None:
            captured_data.append(data)

        wrapped_send = middleware.transporter_send(mock_send)
        test_data = b"A" * 100  # Above threshold

        await wrapped_send("test.topic", test_data, {})

        assert len(captured_data) == 1
        assert captured_data[0][0:1] == FLAG_COMPRESSED
        assert len(captured_data[0]) < len(test_data) + 1  # Compressed + flag

    @pytest.mark.asyncio
    async def test_skip_compression_below_threshold(self, middleware):
        """Test data below threshold is not compressed."""
        captured_data: list[bytes] = []

        async def mock_send(topic: str, data: bytes, meta: dict) -> None:
            captured_data.append(data)

        wrapped_send = middleware.transporter_send(mock_send)
        test_data = b"Short"  # Below threshold (10 bytes)

        await wrapped_send("test.topic", test_data, {})

        assert len(captured_data) == 1
        assert captured_data[0][0:1] == FLAG_NOT_COMPRESSED
        assert captured_data[0][1:] == test_data

    @pytest.mark.asyncio
    async def test_always_compress_when_threshold_zero(self):
        """Test always compress when threshold is 0."""
        middleware = CompressionMiddleware(threshold=0)
        captured_data: list[bytes] = []

        async def mock_send(topic: str, data: bytes, meta: dict) -> None:
            captured_data.append(data)

        wrapped_send = middleware.transporter_send(mock_send)
        test_data = b"Hi"

        await wrapped_send("test.topic", test_data, {})

        assert captured_data[0][0:1] == FLAG_COMPRESSED

    @pytest.mark.asyncio
    async def test_meta_passed_through(self, middleware):
        """Test meta dict is passed to next_send."""
        captured_meta: list[dict] = []

        async def mock_send(topic: str, data: bytes, meta: dict) -> None:
            captured_meta.append(meta)

        wrapped_send = middleware.transporter_send(mock_send)
        test_meta = {"packet": "test"}

        await wrapped_send("test.topic", b"data", test_meta)

        assert captured_meta[0] == test_meta

    @pytest.mark.asyncio
    async def test_large_payload_compression_runs_in_executor(self, middleware, monkeypatch):
        """Large payload compression should be offloaded to executor."""
        loop = asyncio.get_running_loop()
        used_executor = False

        async def fake_send(topic: str, data: bytes, meta: dict) -> None:
            return None

        def fake_run_in_executor(executor, func, *args):
            nonlocal used_executor
            used_executor = True
            fut = loop.create_future()
            fut.set_result(func(*args))
            return fut

        monkeypatch.setattr(loop, "run_in_executor", fake_run_in_executor)
        wrapped_send = middleware.transporter_send(fake_send)
        await wrapped_send("test.topic", b"x" * (2 * 1024 * 1024), {})
        assert used_executor is True


class TestTransporterReceiveHook:
    """Tests for transporter_receive hook."""

    @pytest.fixture
    def middleware(self) -> CompressionMiddleware:
        """Create middleware for testing."""
        return CompressionMiddleware()

    @pytest.mark.asyncio
    async def test_decompress_compressed_data(self, middleware):
        """Test decompressing compressed data."""
        captured_data: list[bytes] = []

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            captured_data.append(data)

        # Prepare compressed data with flag
        original = b"Hello, World! " * 100
        compressed = FLAG_COMPRESSED + middleware._compress(original)

        wrapped_receive = middleware.transporter_receive(mock_receive)
        await wrapped_receive("REQ", compressed, {})

        assert len(captured_data) == 1
        assert captured_data[0] == original

    @pytest.mark.asyncio
    async def test_pass_through_uncompressed_data(self, middleware):
        """Test passing through uncompressed data."""
        captured_data: list[bytes] = []

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            captured_data.append(data)

        # Prepare uncompressed data with flag
        original = b"Short data"
        uncompressed = FLAG_NOT_COMPRESSED + original

        wrapped_receive = middleware.transporter_receive(mock_receive)
        await wrapped_receive("REQ", uncompressed, {})

        assert len(captured_data) == 1
        assert captured_data[0] == original

    @pytest.mark.asyncio
    async def test_empty_packet_passed_through(self, middleware):
        """Test empty packet is passed through."""
        captured_data: list[bytes] = []

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            captured_data.append(data)

        wrapped_receive = middleware.transporter_receive(mock_receive)
        await wrapped_receive("REQ", b"", {})

        assert len(captured_data) == 1
        assert captured_data[0] == b""

    @pytest.mark.asyncio
    async def test_decompression_error_raises(self, middleware):
        """Test decompression error is raised."""
        import zlib

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            pass

        # Invalid compressed data
        invalid_data = FLAG_COMPRESSED + b"invalid"

        wrapped_receive = middleware.transporter_receive(mock_receive)

        with pytest.raises(zlib.error):
            await wrapped_receive("REQ", invalid_data, {})

    @pytest.mark.asyncio
    async def test_meta_passed_through(self, middleware):
        """Test meta dict is passed to next_receive."""
        captured_meta: list[dict] = []

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            captured_meta.append(meta)

        wrapped_receive = middleware.transporter_receive(mock_receive)
        test_meta = {"packet_type": "REQ"}
        data = FLAG_NOT_COMPRESSED + b"test"

        await wrapped_receive("REQ", data, test_meta)

        assert captured_meta[0] == test_meta

    @pytest.mark.asyncio
    async def test_large_payload_decompression_runs_in_executor(self, middleware, monkeypatch):
        """Large payload decompression should be offloaded to executor."""
        loop = asyncio.get_running_loop()
        used_executor = False

        async def fake_receive(cmd: str, data: bytes, meta: dict) -> None:
            return None

        def fake_run_in_executor(executor, func, *args):
            nonlocal used_executor
            used_executor = True
            fut = loop.create_future()
            fut.set_result(func(*args))
            return fut

        payload = b"x" * (2 * 1024 * 1024)
        compressed = FLAG_COMPRESSED + middleware._compress(payload)
        monkeypatch.setattr(loop, "run_in_executor", fake_run_in_executor)
        wrapped_receive = middleware.transporter_receive(fake_receive)
        await wrapped_receive("REQ", compressed, {})
        assert used_executor is True


class TestSendReceiveRoundtrip:
    """Integration tests for send/receive roundtrip."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method",
        [
            CompressionMethod.DEFLATE,
            CompressionMethod.GZIP,
            CompressionMethod.ZLIB,
        ],
    )
    async def test_roundtrip_all_methods(self, method):
        """Test send/receive roundtrip for all compression methods."""
        middleware = CompressionMiddleware(method=method, threshold=10)
        original_data = b"Test data for compression roundtrip " * 10

        # Capture sent data
        sent_data: list[bytes] = []

        async def mock_send(topic: str, data: bytes, meta: dict) -> None:
            sent_data.append(data)

        wrapped_send = middleware.transporter_send(mock_send)
        await wrapped_send("test.topic", original_data, {})

        # Use sent data in receive
        received_data: list[bytes] = []

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            received_data.append(data)

        wrapped_receive = middleware.transporter_receive(mock_receive)
        await wrapped_receive("REQ", sent_data[0], {})

        assert received_data[0] == original_data

    @pytest.mark.asyncio
    async def test_roundtrip_below_threshold(self):
        """Test roundtrip for data below threshold (uncompressed)."""
        middleware = CompressionMiddleware(threshold=1000)
        original_data = b"Short"

        sent_data: list[bytes] = []

        async def mock_send(topic: str, data: bytes, meta: dict) -> None:
            sent_data.append(data)

        wrapped_send = middleware.transporter_send(mock_send)
        await wrapped_send("test.topic", original_data, {})

        received_data: list[bytes] = []

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            received_data.append(data)

        wrapped_receive = middleware.transporter_receive(mock_receive)
        await wrapped_receive("REQ", sent_data[0], {})

        assert received_data[0] == original_data


class TestBrokerCreatedHook:
    """Tests for broker_created hook."""

    def test_broker_created_logs_info(self, caplog):
        """Test broker_created logs compression settings."""
        import logging

        middleware = CompressionMiddleware(method=CompressionMethod.GZIP, threshold=2048)

        with caplog.at_level(logging.INFO):
            middleware.broker_created(None)

        assert "COMPRESSION enabled" in caplog.text
        assert "gzip" in caplog.text
        assert "2048" in caplog.text
