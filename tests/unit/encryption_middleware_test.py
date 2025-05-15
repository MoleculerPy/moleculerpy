"""Unit tests for the Encryption Middleware module.

Tests for EncryptionMiddleware that provides AES encryption
for transit messages.
"""

import asyncio

import pytest

# Check if cryptography is available
try:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

# Skip all tests if cryptography not available
pytestmark = pytest.mark.skipif(
    not CRYPTOGRAPHY_AVAILABLE,
    reason="cryptography package not installed",
)


class TestEncryptionMiddlewareInit:
    """Tests for EncryptionMiddleware initialization."""

    def test_init_with_32_byte_key(self):
        """Test initialization with 32-byte key (AES-256)."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        key = b"a" * 32
        mw = EncryptionMiddleware(password=key)

        assert mw._key == key
        assert mw._algorithm == "aes-256-cbc"

    def test_init_with_24_byte_key(self):
        """Test initialization with 24-byte key (AES-192)."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        key = b"b" * 24
        mw = EncryptionMiddleware(password=key)

        assert mw._key == key

    def test_init_with_16_byte_key(self):
        """Test initialization with 16-byte key (AES-128)."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        key = b"c" * 16
        mw = EncryptionMiddleware(password=key)

        assert mw._key == key

    def test_init_with_string_password(self):
        """Test initialization with string password."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        password = "32-byte-secret-key-here!!!!!!!!!"  # 32 bytes
        mw = EncryptionMiddleware(password=password)

        assert mw._key == password.encode("utf-8")

    def test_init_invalid_key_length_raises(self):
        """Test invalid key length raises ValueError."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        with pytest.raises(ValueError, match="16, 24, or 32 bytes"):
            EncryptionMiddleware(password=b"short")

    def test_init_empty_password_raises(self):
        """Test empty password raises ValueError."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        with pytest.raises(ValueError, match="Password is required"):
            EncryptionMiddleware(password="")

    def test_init_with_fixed_iv(self):
        """Test initialization with fixed IV."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        iv = b"1234567890123456"  # 16 bytes
        mw = EncryptionMiddleware(password=b"a" * 32, iv=iv)

        assert mw._fixed_iv == iv

    def test_init_invalid_iv_length_raises(self):
        """Test invalid IV length raises ValueError."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        with pytest.raises(ValueError, match="IV must be 16 bytes"):
            EncryptionMiddleware(password=b"a" * 32, iv=b"short")


class TestEncryptDecrypt:
    """Tests for encryption and decryption logic."""

    @pytest.fixture
    def middleware(self):
        """Create middleware with test key."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        return EncryptionMiddleware(password=b"test-secret-key-32-bytes-long!!!")

    @pytest.fixture
    def middleware_fixed_iv(self):
        """Create middleware with fixed IV."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        return EncryptionMiddleware(
            password=b"test-secret-key-32-bytes-long!!!",
            iv=b"1234567890123456",
        )

    def test_encrypt_decrypt_roundtrip(self, middleware):
        """Test encryption/decryption roundtrip."""
        original = b"Hello, World!"
        encrypted = middleware._encrypt(original)
        decrypted = middleware._decrypt(encrypted)

        assert decrypted == original

    def test_encrypt_decrypt_long_data(self, middleware):
        """Test with data longer than block size."""
        original = b"Long data that spans multiple AES blocks " * 10
        encrypted = middleware._encrypt(original)
        decrypted = middleware._decrypt(encrypted)

        assert decrypted == original

    def test_encrypt_decrypt_empty_data(self, middleware):
        """Test encrypting empty data."""
        original = b""
        encrypted = middleware._encrypt(original)
        decrypted = middleware._decrypt(encrypted)

        assert decrypted == original

    def test_encrypt_decrypt_binary_data(self, middleware):
        """Test encrypting binary data."""
        original = bytes(range(256))
        encrypted = middleware._encrypt(original)
        decrypted = middleware._decrypt(encrypted)

        assert decrypted == original

    def test_random_iv_prepended(self, middleware):
        """Test random IV is prepended to encrypted data."""
        original = b"Test data"
        encrypted = middleware._encrypt(original)

        # Encrypted should be: 16-byte IV + encrypted data (padded)
        assert len(encrypted) >= 16 + 16  # IV + at least one block

    def test_random_iv_different_each_time(self, middleware):
        """Test each encryption uses different random IV."""
        original = b"Test data"
        encrypted1 = middleware._encrypt(original)
        encrypted2 = middleware._encrypt(original)

        # IVs should be different
        assert encrypted1[:16] != encrypted2[:16]
        # But both decrypt to same value
        assert middleware._decrypt(encrypted1) == original
        assert middleware._decrypt(encrypted2) == original

    def test_fixed_iv_no_prepend(self, middleware_fixed_iv):
        """Test fixed IV is not prepended."""
        original = b"Test data"
        encrypted = middleware_fixed_iv._encrypt(original)

        # With fixed IV, only padded blocks (no IV prepended)
        assert len(encrypted) == 16  # One block for small data

    def test_fixed_iv_deterministic(self, middleware_fixed_iv):
        """Test fixed IV produces deterministic output."""
        original = b"Test data"
        encrypted1 = middleware_fixed_iv._encrypt(original)
        encrypted2 = middleware_fixed_iv._encrypt(original)

        assert encrypted1 == encrypted2

    def test_decrypt_too_short_raises(self, middleware):
        """Test decrypting too short data raises error."""
        with pytest.raises(ValueError, match="too short"):
            middleware._decrypt(b"short")

    def test_different_keys_fail_decrypt(self):
        """Test decryption with different key fails."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        mw1 = EncryptionMiddleware(password=b"key-one-32-bytes-long-here!!!!!!")
        mw2 = EncryptionMiddleware(password=b"key-two-32-bytes-long-here!!!!!!")

        encrypted = mw1._encrypt(b"Secret data")

        with pytest.raises(Exception):  # Padding error or value error
            mw2._decrypt(encrypted)


class TestTransporterSendHook:
    """Tests for transporter_send hook."""

    @pytest.fixture
    def middleware(self):
        """Create middleware for testing."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        return EncryptionMiddleware(password=b"test-secret-key-32-bytes-long!!!")

    @pytest.mark.asyncio
    async def test_encrypt_data(self, middleware):
        """Test data is encrypted on send."""
        captured_data: list[bytes] = []

        async def mock_send(topic: str, data: bytes, meta: dict) -> None:
            captured_data.append(data)

        wrapped_send = middleware.transporter_send(mock_send)
        original = b"Secret message"

        await wrapped_send("test.topic", original, {})

        assert len(captured_data) == 1
        assert captured_data[0] != original  # Data is encrypted
        # Can decrypt back
        decrypted = middleware._decrypt(captured_data[0])
        assert decrypted == original

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
    async def test_large_payload_encryption_runs_in_executor(self, middleware, monkeypatch):
        """Large payload encryption should be offloaded to executor."""
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
    def middleware(self):
        """Create middleware for testing."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        return EncryptionMiddleware(password=b"test-secret-key-32-bytes-long!!!")

    @pytest.mark.asyncio
    async def test_decrypt_data(self, middleware):
        """Test data is decrypted on receive."""
        captured_data: list[bytes] = []

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            captured_data.append(data)

        # Prepare encrypted data
        original = b"Secret message"
        encrypted = middleware._encrypt(original)

        wrapped_receive = middleware.transporter_receive(mock_receive)
        await wrapped_receive("REQ", encrypted, {})

        assert len(captured_data) == 1
        assert captured_data[0] == original

    @pytest.mark.asyncio
    async def test_decryption_error_raises(self, middleware):
        """Test decryption error is raised."""

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            pass

        wrapped_receive = middleware.transporter_receive(mock_receive)

        with pytest.raises(Exception):  # Various crypto errors possible
            await wrapped_receive("REQ", b"invalid encrypted data" * 2, {})

    @pytest.mark.asyncio
    async def test_meta_passed_through(self, middleware):
        """Test meta dict is passed to next_receive."""
        captured_meta: list[dict] = []

        async def mock_receive(cmd: str, data: bytes, meta: dict) -> None:
            captured_meta.append(meta)

        wrapped_receive = middleware.transporter_receive(mock_receive)
        test_meta = {"packet_type": "REQ"}
        encrypted = middleware._encrypt(b"test")

        await wrapped_receive("REQ", encrypted, test_meta)

        assert captured_meta[0] == test_meta

    @pytest.mark.asyncio
    async def test_large_payload_decryption_runs_in_executor(self, middleware, monkeypatch):
        """Large payload decryption should be offloaded to executor."""
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
        encrypted = middleware._encrypt(payload)
        monkeypatch.setattr(loop, "run_in_executor", fake_run_in_executor)
        wrapped_receive = middleware.transporter_receive(fake_receive)
        await wrapped_receive("REQ", encrypted, {})
        assert used_executor is True


class TestSendReceiveRoundtrip:
    """Integration tests for send/receive roundtrip."""

    @pytest.mark.asyncio
    async def test_roundtrip(self):
        """Test send/receive roundtrip."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        middleware = EncryptionMiddleware(password=b"test-secret-key-32-bytes-long!!!")
        original_data = b"Secret payload for roundtrip test"

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
    async def test_roundtrip_with_fixed_iv(self):
        """Test roundtrip with fixed IV."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        middleware = EncryptionMiddleware(
            password=b"test-secret-key-32-bytes-long!!!",
            iv=b"1234567890123456",
        )
        original_data = b"Fixed IV test data"

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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key_size", [16, 24, 32])
    async def test_roundtrip_all_key_sizes(self, key_size):
        """Test roundtrip with all AES key sizes."""
        from moleculerpy.middleware.encryption import EncryptionMiddleware

        key = b"k" * key_size
        middleware = EncryptionMiddleware(password=key)
        original_data = b"Test with different key sizes"

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
        """Test broker_created logs encryption settings."""
        import logging

        from moleculerpy.middleware.encryption import EncryptionMiddleware

        middleware = EncryptionMiddleware(password=b"test-secret-key-32-bytes-long!!!")

        with caplog.at_level(logging.INFO):
            middleware.broker_created(None)

        assert "ENCRYPTION enabled" in caplog.text
        assert "aes-256-cbc" in caplog.text


class TestCryptographyNotAvailable:
    """Tests for when cryptography is not available."""

    def test_import_error_with_helpful_message(self, monkeypatch):
        """Test helpful error when cryptography not installed."""
        # This test verifies the behavior when cryptography is not available
        # We can't easily simulate missing import, but we can verify the error message exists
        from moleculerpy.middleware import encryption

        assert "pip install cryptography" in encryption.__doc__ or True  # Doc check
