"""Unit tests for the settings module."""

import pytest

from moleculerpy.settings import Settings, SettingsValidationError


class TestSettings:
    """Test Settings class."""

    def test_settings_default_initialization(self):
        """Test Settings initialization with default values."""
        settings = Settings()

        assert settings.transporter == "nats://localhost:4222"
        assert settings.serializer == "JSON"
        assert settings.log_level == "INFO"
        assert settings.log_format == "PLAIN"
        assert settings.middlewares == []

    def test_settings_custom_initialization(self):
        """Test Settings initialization with custom values."""
        custom_middlewares = ["middleware1", "middleware2"]

        settings = Settings(
            transporter="redis://localhost:6379",
            serializer="MSGPACK",
            log_level="DEBUG",
            log_format="JSON",
            middlewares=custom_middlewares,
        )

        assert settings.transporter == "redis://localhost:6379"
        assert settings.serializer == "MSGPACK"
        assert settings.log_level == "DEBUG"
        assert settings.log_format == "JSON"
        assert settings.middlewares == custom_middlewares

    def test_settings_partial_initialization(self):
        """Test Settings initialization with some custom values."""
        settings = Settings(transporter="tcp://localhost:8080", log_level="ERROR")

        assert settings.transporter == "tcp://localhost:8080"
        assert settings.log_level == "ERROR"
        # Others should use defaults
        assert settings.serializer == "JSON"
        assert settings.log_format == "PLAIN"
        assert settings.middlewares == []

    def test_settings_empty_middlewares(self):
        """Test Settings with explicitly empty middlewares."""
        settings = Settings(middlewares=[])

        assert settings.middlewares == []

    def test_settings_none_middlewares(self):
        """Test Settings with None middlewares (should default to empty list)."""
        settings = Settings(middlewares=None)

        assert settings.middlewares == []

    def test_settings_complex_middlewares(self):
        """Test Settings with complex middleware objects."""

        def middleware_func():
            pass

        class MiddlewareClass:
            pass

        middlewares = [
            middleware_func,
            MiddlewareClass(),
            "string_middleware",
            {"type": "dict_middleware", "config": {"timeout": 5000}},
        ]

        settings = Settings(middlewares=middlewares)

        assert settings.middlewares == middlewares
        assert len(settings.middlewares) == 4

    def test_settings_different_transporter_urls(self):
        """Test Settings with different transporter URL formats."""
        transporter_urls = [
            "nats://localhost:4222",
            "redis://redis.example.com:6379",
            "tcp://10.0.0.1:8080",
            "amqp://user:pass@rabbitmq.local:5672",
            "ws://websocket-server:3000",
            "custom-protocol://host:port",
        ]

        for url in transporter_urls:
            settings = Settings(transporter=url)
            assert settings.transporter == url

    def test_settings_different_log_levels(self):
        """Test Settings with different log levels."""
        log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        for level in log_levels:
            settings = Settings(log_level=level)
            assert settings.log_level == level

    def test_settings_different_log_formats(self):
        """Test Settings with different valid log formats."""
        log_formats = ["PLAIN", "JSON"]

        for format_type in log_formats:
            settings = Settings(log_format=format_type)
            assert settings.log_format == format_type

    def test_settings_invalid_log_format_raises_error(self):
        """Test that invalid log format raises SettingsValidationError."""
        with pytest.raises(SettingsValidationError) as exc_info:
            Settings(log_format="XML")

        assert "log_format" in str(exc_info.value)
        assert "PLAIN" in str(exc_info.value) or "JSON" in str(exc_info.value)

    def test_settings_invalid_log_level_raises_error(self):
        """Test that invalid log level raises SettingsValidationError."""
        with pytest.raises(SettingsValidationError) as exc_info:
            Settings(log_level="VERBOSE")

        assert "log_level" in str(exc_info.value)

    def test_settings_different_serializers(self):
        """Test Settings with different serializers."""
        # Only JSON and MSGPACK are valid serializers
        valid_serializers = ["JSON", "MSGPACK"]

        for serializer in valid_serializers:
            settings = Settings(serializer=serializer)
            assert settings.serializer == serializer

    def test_settings_attribute_modification(self):
        """Test that Settings attributes can be modified after creation."""
        settings = Settings()

        # Modify all attributes (validation only happens in __init__)
        settings.transporter = "new://localhost:9999"
        settings.serializer = "NEW_SERIALIZER"
        settings.log_level = "CRITICAL"
        settings.log_format = "CUSTOM"
        settings.middlewares = ["new_middleware"]

        assert settings.transporter == "new://localhost:9999"
        assert settings.serializer == "NEW_SERIALIZER"
        assert settings.log_level == "CRITICAL"
        assert settings.log_format == "CUSTOM"
        assert settings.middlewares == ["new_middleware"]

    def test_settings_middlewares_list_operations(self):
        """Test that middlewares list can be manipulated."""
        settings = Settings()

        # Start with empty list
        assert len(settings.middlewares) == 0

        # Add middlewares
        settings.middlewares.append("middleware1")
        settings.middlewares.extend(["middleware2", "middleware3"])

        assert len(settings.middlewares) == 3
        assert "middleware1" in settings.middlewares
        assert "middleware2" in settings.middlewares
        assert "middleware3" in settings.middlewares

        # Remove middleware
        settings.middlewares.remove("middleware2")
        assert len(settings.middlewares) == 2
        assert "middleware2" not in settings.middlewares

    def test_settings_case_insensitive_log_level(self):
        """Test that log level validation is case insensitive."""
        # Lowercase should be accepted
        settings = Settings(log_level="debug")
        assert settings.log_level == "debug"

        settings = Settings(log_level="Info")
        assert settings.log_level == "Info"

    def test_settings_case_insensitive_log_format(self):
        """Test that log format validation is case insensitive."""
        # Lowercase should be accepted
        settings = Settings(log_format="json")
        assert settings.log_format == "json"

        settings = Settings(log_format="Plain")
        assert settings.log_format == "Plain"

    def test_settings_special_characters_in_transporter(self):
        """Test Settings with special characters in transporter URL."""
        settings = Settings(
            transporter="custom://user:p@ssw0rd@host.example.com:8080/path?query=value&other=123"
        )

        assert "p@ssw0rd" in settings.transporter

    def test_settings_invalid_transporter_format(self):
        """Test that invalid transporter format raises error."""
        with pytest.raises(SettingsValidationError) as exc_info:
            Settings(transporter="localhost:4222")  # Missing protocol://

        assert "transporter" in str(exc_info.value)
        assert "URL" in str(exc_info.value)

    def test_settings_with_callable_middlewares(self):
        """Test Settings with callable middlewares."""

        def sync_middleware():
            return "sync"

        async def async_middleware():
            return "async"

        class CallableMiddleware:
            def __call__(self):
                return "callable"

        middlewares = [sync_middleware, async_middleware, CallableMiddleware()]
        settings = Settings(middlewares=middlewares)

        assert callable(settings.middlewares[0])
        assert callable(settings.middlewares[1])
        assert callable(settings.middlewares[2])

        # Test that they can be called
        assert settings.middlewares[0]() == "sync"
        assert settings.middlewares[2]() == "callable"

    def test_settings_timeout_defaults(self):
        """Test Settings default timeout values."""
        settings = Settings()

        assert settings.request_timeout == 30.0
        assert settings.heartbeat_interval == 5.0
        assert settings.heartbeat_timeout == 15.0

    def test_settings_timeout_custom(self):
        """Test Settings with custom timeout values."""
        settings = Settings(
            request_timeout=120.0,  # 2 minutes for LLM calls
            graceful_stop_timeout=20.0,
            heartbeat_interval=10.0,
            heartbeat_timeout=30.0,
        )

        assert settings.request_timeout == 120.0
        assert settings.graceful_stop_timeout == 20.0
        assert settings.heartbeat_interval == 10.0
        assert settings.heartbeat_timeout == 30.0

    def test_settings_graceful_stop_timeout_default(self):
        """graceful_stop_timeout should be opt-in and default to None."""
        settings = Settings()
        assert settings.graceful_stop_timeout is None

    def test_settings_invalid_graceful_stop_timeout_raises_error(self):
        """Non-positive graceful_stop_timeout should fail validation."""
        with pytest.raises(SettingsValidationError, match="graceful_stop_timeout"):
            Settings(graceful_stop_timeout=0.0)

    def test_settings_default_timeout_alias(self):
        """Test that default_timeout is an alias for request_timeout."""
        settings = Settings(request_timeout=60.0)

        assert settings.default_timeout == 60.0
        assert settings.default_timeout == settings.request_timeout

    def test_settings_timeout_float_precision(self):
        """Test Settings with precise float timeout values."""
        settings = Settings(
            request_timeout=0.5,  # 500ms
            heartbeat_interval=0.1,  # 100ms
            heartbeat_timeout=1.5,  # 1.5s
        )

        assert settings.request_timeout == 0.5
        assert settings.heartbeat_interval == 0.1
        assert settings.heartbeat_timeout == 1.5

    def test_settings_invalid_request_timeout(self):
        """Test that non-positive request_timeout raises error."""
        with pytest.raises(SettingsValidationError) as exc_info:
            Settings(request_timeout=0)

        assert "request_timeout" in str(exc_info.value)
        assert "positive" in str(exc_info.value)

        with pytest.raises(SettingsValidationError):
            Settings(request_timeout=-1)

    def test_settings_invalid_heartbeat_interval(self):
        """Test that non-positive heartbeat_interval raises error."""
        with pytest.raises(SettingsValidationError) as exc_info:
            Settings(heartbeat_interval=0)

        assert "heartbeat_interval" in str(exc_info.value)
        assert "positive" in str(exc_info.value)

    def test_settings_invalid_heartbeat_timeout(self):
        """Test that non-positive heartbeat_timeout raises error."""
        with pytest.raises(SettingsValidationError) as exc_info:
            Settings(heartbeat_timeout=0)

        assert "heartbeat_timeout" in str(exc_info.value)
        assert "positive" in str(exc_info.value)

    def test_settings_heartbeat_timeout_must_be_greater_than_interval(self):
        """Test that heartbeat_timeout must be > heartbeat_interval."""
        with pytest.raises(SettingsValidationError) as exc_info:
            Settings(heartbeat_interval=10.0, heartbeat_timeout=5.0)

        assert "heartbeat_timeout" in str(exc_info.value)
        assert "heartbeat_interval" in str(exc_info.value)

        # Equal values should also fail
        with pytest.raises(SettingsValidationError):
            Settings(heartbeat_interval=10.0, heartbeat_timeout=10.0)

    def test_settings_valid_constants(self):
        """Test that Settings has correct validation constants."""
        assert "DEBUG" in Settings.VALID_LOG_LEVELS
        assert "INFO" in Settings.VALID_LOG_LEVELS
        assert "WARNING" in Settings.VALID_LOG_LEVELS
        assert "ERROR" in Settings.VALID_LOG_LEVELS
        assert "CRITICAL" in Settings.VALID_LOG_LEVELS

        assert "PLAIN" in Settings.VALID_LOG_FORMATS
        assert "JSON" in Settings.VALID_LOG_FORMATS
