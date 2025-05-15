"""Unit tests for the RetryMiddleware."""

import asyncio
from unittest.mock import MagicMock

import pytest

from moleculerpy.middleware.retry import RetryMiddleware
from moleculerpy.transit import RemoteCallError


class TestRetryMiddlewareInit:
    """Test RetryMiddleware initialization."""

    def test_default_initialization(self):
        """Test RetryMiddleware with default values."""
        middleware = RetryMiddleware()

        assert middleware.max_retries == 3
        assert middleware.base_delay == 0.5
        assert middleware.max_delay == 30.0
        assert middleware.exponential_base == 2.0
        assert middleware.jitter is True

    def test_custom_initialization(self):
        """Test RetryMiddleware with custom values."""
        middleware = RetryMiddleware(
            max_retries=5,
            base_delay=1.0,
            max_delay=60.0,
            exponential_base=3.0,
            jitter=False,
        )

        assert middleware.max_retries == 5
        assert middleware.base_delay == 1.0
        assert middleware.max_delay == 60.0
        assert middleware.exponential_base == 3.0
        assert middleware.jitter is False

    def test_custom_retryable_errors(self):
        """Test RetryMiddleware with custom retryable errors."""
        custom_errors = {ValueError, KeyError}
        middleware = RetryMiddleware(retryable_errors=custom_errors)

        assert middleware.retryable_errors == custom_errors


class TestRetryMiddlewareDelayCalculation:
    """Test delay calculation logic."""

    def test_calculate_delay_without_jitter(self):
        """Test exponential backoff delay calculation without jitter."""
        middleware = RetryMiddleware(base_delay=1.0, exponential_base=2.0, jitter=False)

        assert middleware._calculate_delay(0) == 1.0  # 1 * 2^0 = 1
        assert middleware._calculate_delay(1) == 2.0  # 1 * 2^1 = 2
        assert middleware._calculate_delay(2) == 4.0  # 1 * 2^2 = 4
        assert middleware._calculate_delay(3) == 8.0  # 1 * 2^3 = 8

    def test_calculate_delay_respects_max_delay(self):
        """Test that delay is capped at max_delay."""
        middleware = RetryMiddleware(
            base_delay=1.0, max_delay=5.0, exponential_base=2.0, jitter=False
        )

        assert middleware._calculate_delay(0) == 1.0
        assert middleware._calculate_delay(1) == 2.0
        assert middleware._calculate_delay(2) == 4.0
        assert middleware._calculate_delay(3) == 5.0  # Capped at max_delay
        assert middleware._calculate_delay(10) == 5.0  # Still capped

    def test_calculate_delay_with_jitter(self):
        """Test that jitter adds randomness to delay."""
        middleware = RetryMiddleware(base_delay=1.0, exponential_base=2.0, jitter=True)

        # Run multiple times to verify randomness
        delays = [middleware._calculate_delay(1) for _ in range(10)]

        # Base delay for attempt 1 is 2.0
        # With jitter, it should be between 2.0 and 2.5 (2.0 + 25% jitter)
        for delay in delays:
            assert 2.0 <= delay <= 2.5

        # Should have some variance (not all same)
        assert len(set(delays)) > 1


class TestRetryMiddlewareRetryableCheck:
    """Test retryable error checking."""

    def test_timeout_error_is_retryable(self):
        """Test that TimeoutError is retryable by default."""
        middleware = RetryMiddleware()

        assert middleware._is_retryable(TimeoutError())

    def test_remote_call_error_is_retryable(self):
        """Test that RemoteCallError is retryable by default."""
        middleware = RetryMiddleware()

        assert middleware._is_retryable(RemoteCallError("test error"))

    def test_connection_error_is_retryable(self):
        """Test that ConnectionError is retryable by default."""
        middleware = RetryMiddleware()

        assert middleware._is_retryable(ConnectionError())

    def test_value_error_is_not_retryable(self):
        """Test that ValueError is not retryable by default."""
        middleware = RetryMiddleware()

        assert not middleware._is_retryable(ValueError("test"))

    def test_custom_retryable_errors(self):
        """Test custom retryable errors."""
        middleware = RetryMiddleware(retryable_errors={ValueError, KeyError})

        assert middleware._is_retryable(ValueError("test"))
        assert middleware._is_retryable(KeyError("test"))
        assert not middleware._is_retryable(TimeoutError())


class TestRetryMiddlewarePerActionRetries:
    """Test per-action retry settings."""

    def test_action_retries_override(self):
        """Test that action-specific retries override middleware default."""
        middleware = RetryMiddleware(max_retries=3)

        # Action with custom retries
        action_with_retries = MagicMock()
        action_with_retries.retries = 5

        assert middleware._get_max_retries_for_action(action_with_retries) == 5

    def test_action_without_retries_uses_default(self):
        """Test that action without retries uses middleware default."""
        middleware = RetryMiddleware(max_retries=3)

        # Action without custom retries
        action_without_retries = MagicMock(spec=[])  # No retries attribute

        assert middleware._get_max_retries_for_action(action_without_retries) == 3

    def test_action_with_none_retries_uses_default(self):
        """Test that action with None retries uses middleware default."""
        middleware = RetryMiddleware(max_retries=3)

        action_with_none = MagicMock()
        action_with_none.retries = None

        assert middleware._get_max_retries_for_action(action_with_none) == 3

    def test_action_with_zero_retries(self):
        """Test that action with 0 retries disables retries."""
        middleware = RetryMiddleware(max_retries=3)

        action_with_zero = MagicMock()
        action_with_zero.retries = 0

        assert middleware._get_max_retries_for_action(action_with_zero) == 0


@pytest.mark.asyncio
class TestRetryMiddlewareRemoteAction:
    """Test remote_action hook with retry logic."""

    async def test_successful_call_no_retry(self):
        """Test that successful calls don't trigger retries."""
        middleware = RetryMiddleware(max_retries=3)

        # Mock action and handler
        action = MagicMock()
        action.name = "test.action"
        action.retries = None

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            return "success"

        # Get wrapped handler
        handler = await middleware.remote_action(mock_handler, action)

        # Call the handler
        ctx = MagicMock()
        result = await handler(ctx)

        assert result == "success"
        assert call_count == 1  # Only called once

    async def test_retry_on_timeout_error(self):
        """Test retry on TimeoutError."""
        middleware = RetryMiddleware(max_retries=2, base_delay=0.01, jitter=False)

        action = MagicMock()
        action.name = "test.action"
        action.retries = None

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TimeoutError("timeout")
            return "success"

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.logger = MagicMock()

        result = await handler(ctx)

        assert result == "success"
        assert call_count == 3  # Initial + 2 retries

    async def test_retry_on_remote_call_error(self):
        """Test retry on RemoteCallError."""
        middleware = RetryMiddleware(max_retries=2, base_delay=0.01, jitter=False)

        action = MagicMock()
        action.name = "test.action"
        action.retries = None

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RemoteCallError("remote error")
            return "success"

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.logger = MagicMock()

        result = await handler(ctx)

        assert result == "success"
        assert call_count == 2

    async def test_no_retry_on_non_retryable_error(self):
        """Test that non-retryable errors are not retried."""
        middleware = RetryMiddleware(max_retries=3, base_delay=0.01)

        action = MagicMock()
        action.name = "test.action"
        action.retries = None

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()

        with pytest.raises(ValueError, match="not retryable"):
            await handler(ctx)

        assert call_count == 1  # Only called once

    async def test_exhaust_all_retries(self):
        """Test that error is raised when all retries are exhausted."""
        middleware = RetryMiddleware(max_retries=2, base_delay=0.01, jitter=False)

        action = MagicMock()
        action.name = "test.action"
        action.retries = None

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            raise TimeoutError("always times out")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.logger = MagicMock()

        with pytest.raises(asyncio.TimeoutError, match="always times out"):
            await handler(ctx)

        assert call_count == 3  # Initial + 2 retries

    async def test_per_action_retry_override(self):
        """Test per-action retry override."""
        middleware = RetryMiddleware(max_retries=5, base_delay=0.01, jitter=False)

        # Action with custom retries = 1
        action = MagicMock()
        action.name = "test.action"
        action.retries = 1

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            raise TimeoutError("timeout")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.logger = MagicMock()

        with pytest.raises(asyncio.TimeoutError):
            await handler(ctx)

        # Should only retry once (action.retries=1), not 5 times
        assert call_count == 2  # Initial + 1 retry

    async def test_zero_retries_disables_retry(self):
        """Test that 0 retries disables retry logic."""
        middleware = RetryMiddleware(max_retries=3, base_delay=0.01)

        action = MagicMock()
        action.name = "test.action"
        action.retries = 0  # Disable retries

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            raise TimeoutError("timeout")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()

        with pytest.raises(asyncio.TimeoutError):
            await handler(ctx)

        assert call_count == 1  # No retries
