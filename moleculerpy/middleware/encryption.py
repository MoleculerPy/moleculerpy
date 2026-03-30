"""
Encryption Middleware for the MoleculerPy framework.

This middleware encrypts transit messages to protect the whole
MoleculerPy transporter communication with AES encryption.

Example:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware.encryption import EncryptionMiddleware

    settings = Settings(
        middlewares=[
            EncryptionMiddleware(
                password="my-secret-key-32-bytes-long!!!!",
                algorithm="aes-256-cbc"
            )
        ]
    )
    broker = ServiceBroker("node-1", settings=settings)
"""

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from .base import Middleware

# Optional dependency - lazy import
if TYPE_CHECKING:
    from cryptography.hazmat.primitives.ciphers import Cipher

try:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

logger = logging.getLogger(__name__)


class EncryptionMiddleware(Middleware):
    """Middleware that encrypts transit messages using AES encryption.

    This middleware wraps the transporter send/receive methods to encrypt
    outgoing messages and decrypt incoming messages.

    Args:
        password: Secret key for encryption. Must be 16, 24, or 32 bytes
            for AES-128, AES-192, or AES-256 respectively.
        algorithm: Encryption algorithm. Currently only "aes-256-cbc" is supported.
        iv: Optional initialization vector (16 bytes). If not provided,
            a random IV is generated for each message and prepended.

    Example:
        # Generate a 32-byte key for AES-256
        key = secrets.token_bytes(32)
        middleware = EncryptionMiddleware(password=key)
    """

    _EXECUTOR_THRESHOLD_BYTES = 1024 * 1024
    _AES_BLOCK_SIZE_BYTES = 16

    def __init__(
        self,
        password: str | bytes,
        algorithm: str = "aes-256-cbc",
        iv: bytes | None = None,
    ) -> None:
        """Initialize the encryption middleware.

        Args:
            password: Secret key (16, 24, or 32 bytes for AES-128/192/256)
            algorithm: Encryption algorithm (default: aes-256-cbc)
            iv: Optional fixed IV (16 bytes). If None, random IV per message.

        Raises:
            ValueError: If password is empty or wrong length
            ImportError: If cryptography package is not installed
        """
        if not CRYPTOGRAPHY_AVAILABLE:
            raise ImportError(
                "EncryptionMiddleware requires the 'cryptography' package. "
                "Install it with: pip install cryptography"
            )

        if not password:
            raise ValueError("Password is required for encryption")

        # Convert string password to bytes
        if isinstance(password, str):
            password = password.encode("utf-8")

        # Validate key length
        if len(password) not in (16, 24, 32):
            raise ValueError(f"Password must be 16, 24, or 32 bytes for AES, got {len(password)}")

        self._key = password
        self._algorithm = algorithm
        self._fixed_iv = iv

        # Validate fixed IV if provided
        if iv is not None and len(iv) != self._AES_BLOCK_SIZE_BYTES:
            raise ValueError(f"IV must be {self._AES_BLOCK_SIZE_BYTES} bytes, got {len(iv)}")

        logger.info(
            "Encryption middleware initialized: algorithm=%s, key_size=%d bits",
            algorithm,
            len(password) * 8,
        )

    def _encrypt(self, data: bytes) -> bytes:
        """Encrypt data using AES-CBC.

        If no fixed IV is set, generates a random IV and prepends it.

        Args:
            data: Raw bytes to encrypt

        Returns:
            Encrypted bytes (with IV prepended if using random IV)
        """
        # Use fixed IV or generate random
        if self._fixed_iv:
            iv = self._fixed_iv
            prepend_iv = False
        else:
            iv = secrets.token_bytes(self._AES_BLOCK_SIZE_BYTES)
            prepend_iv = True

        # Pad data to block size
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(data) + padder.finalize()

        # Encrypt
        cipher = Cipher(algorithms.AES(self._key), modes.CBC(iv))
        encryptor = cipher.encryptor()
        encrypted = encryptor.update(padded_data) + encryptor.finalize()

        # Prepend IV if using random IV
        if prepend_iv:
            return bytes(iv + encrypted)
        return bytes(encrypted)

    def _decrypt(self, data: bytes) -> bytes:
        """Decrypt data using AES-CBC.

        If no fixed IV is set, extracts IV from beginning of data.

        Args:
            data: Encrypted bytes (with IV if using random IV)

        Returns:
            Decrypted bytes

        Raises:
            ValueError: If data is too short or decryption fails
        """
        # Extract IV
        if self._fixed_iv:
            iv = self._fixed_iv
            encrypted = data
        else:
            if len(data) < self._AES_BLOCK_SIZE_BYTES:
                raise ValueError("Encrypted data too short (missing IV)")
            iv = data[: self._AES_BLOCK_SIZE_BYTES]
            encrypted = data[self._AES_BLOCK_SIZE_BYTES :]

        # Decrypt
        cipher = Cipher(algorithms.AES(self._key), modes.CBC(iv))
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(encrypted) + decryptor.finalize()

        # Unpad
        unpadder = padding.PKCS7(128).unpadder()
        return bytes(unpadder.update(padded_data) + unpadder.finalize())

    def broker_created(self, broker: Any) -> None:
        """Log encryption settings when broker is created.

        Args:
            broker: The broker instance
        """
        logger.info(
            "Transmission ENCRYPTION enabled: algorithm='%s'",
            self._algorithm,
        )

    def transporter_send(
        self, next_send: Callable[[str, bytes, dict[str, Any]], Awaitable[None]]
    ) -> Callable[[str, bytes, dict[str, Any]], Awaitable[None]]:
        """Wrap transporter send to encrypt outgoing data.

        Args:
            next_send: The next send function in the chain

        Returns:
            Wrapped send function
        """

        async def send(topic: str, data: bytes, meta: dict[str, Any]) -> None:
            if len(data) >= self._EXECUTOR_THRESHOLD_BYTES:
                loop = asyncio.get_running_loop()
                encrypted = await loop.run_in_executor(None, self._encrypt, data)
            else:
                encrypted = self._encrypt(data)
            logger.debug(
                "Packet '%s' encrypted: %d -> %d bytes",
                topic,
                len(data),
                len(encrypted),
            )
            return await next_send(topic, encrypted, meta)

        return send

    def transporter_receive(
        self, next_receive: Callable[[str, bytes, dict[str, Any]], Awaitable[None]]
    ) -> Callable[[str, bytes, dict[str, Any]], Awaitable[None]]:
        """Wrap transporter receive to decrypt incoming data.

        Args:
            next_receive: The next receive function in the chain

        Returns:
            Wrapped receive function
        """

        async def receive(cmd: str, data: bytes, meta: dict[str, Any]) -> None:
            try:
                if len(data) >= self._EXECUTOR_THRESHOLD_BYTES:
                    loop = asyncio.get_running_loop()
                    decrypted = await loop.run_in_executor(None, self._decrypt, data)
                else:
                    decrypted = self._decrypt(data)
                logger.debug(
                    "Packet '%s' decrypted: %d -> %d bytes",
                    cmd,
                    len(data),
                    len(decrypted),
                )
                return await next_receive(cmd, decrypted, meta)

            except Exception as e:
                logger.error(
                    "Decryption failed for packet '%s': %s",
                    cmd,
                    e,
                )
                raise

        return receive
