"""ActionLogger Middleware for MoleculerPy.

Logs action calls, responses, and errors to console and/or files.
Compatible with Moleculer.js ActionLogger middleware.

Example:
    >>> from moleculerpy.middlewares import ActionLogger
    >>> broker = ServiceBroker(middlewares=[
    ...     ActionLogger(
    ...         log_params=True,
    ...         log_response=True,
    ...         folder="./logs",
    ...         whitelist=["api.**", "iam.**"]
    ...     )
    ... ])
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import traceback
from collections.abc import Callable
from functools import lru_cache
from logging import Logger
from pathlib import Path
from re import Pattern
from typing import TYPE_CHECKING, Any, Literal

from ..middleware import Middleware

if TYPE_CHECKING:
    from ..broker import ServiceBroker
    from ..context import Context

# Type alias for log levels
LogLevel = Literal["debug", "info", "warn", "error"]


@lru_cache(maxsize=128)
def _compile_glob_pattern(pattern: str) -> Pattern[str]:
    """Compile glob pattern to regex (cached).

    Caches compiled patterns to avoid repeated regex compilation.

    Args:
        pattern: Glob pattern (e.g., "api.**", "iam.user.*")

    Returns:
        Compiled regex pattern
    """
    # Convert glob to regex
    regex = pattern.replace(".", r"\.")  # Escape dots
    regex = regex.replace("**", "___DOUBLESTAR___")  # Temp placeholder
    regex = regex.replace("*", r"[^.]*")  # * matches non-dots
    regex = regex.replace("___DOUBLESTAR___", ".*")  # ** matches anything
    regex = regex.replace("?", ".")  # ? matches one char
    return re.compile(f"^{regex}$")


def match_pattern(pattern: str, text: str) -> bool:
    """Match glob-style pattern against text.

    Uses cached compiled regex patterns for performance.

    Supports:
        - `*` matches any characters except dots
        - `**` matches any characters including dots
        - `?` matches single character

    Args:
        pattern: Glob pattern (e.g., "api.**", "iam.user.*")
        text: Text to match (e.g., "api.v1.user.list")

    Returns:
        True if text matches pattern
    """
    return bool(_compile_glob_pattern(pattern).match(text))


class ActionLogger(Middleware):
    """Middleware for logging action calls, responses, and errors.

    Provides dual output: console (via broker logger) and file system (JSON).
    Compatible with Moleculer.js ActionLogger configuration.

    Configuration:
        log_params: Log action parameters (default: False)
        log_response: Log responses and errors (default: False)
        log_meta: Log context metadata (default: False)
        log_level: Logger level to use - "debug", "info", "warn", "error" (default: "info")
        folder: Directory for file logs, None to disable (default: None)
        extension: File extension for logs (default: ".json")
        whitelist: Action name patterns to log (default: ["**"])

    Example:
        >>> broker = ServiceBroker(middlewares=[
        ...     ActionLogger(
        ...         log_params=True,
        ...         log_response=True,
        ...         log_meta=True,
        ...         folder="./logs",
        ...         whitelist=["api.**", "iam.**"]
        ...     )
        ... ])

    File naming:
        {timestamp}-call-{action_name}-{type}.json

        Types: request, meta, response, error
    """

    def __init__(
        self,
        *,
        log_params: bool = False,
        log_response: bool = False,
        log_meta: bool = False,
        log_level: LogLevel = "info",
        folder: str | Path | None = None,
        extension: str = ".json",
        whitelist: list[str] | None = None,
    ) -> None:
        """Initialize ActionLogger middleware.

        Args:
            log_params: Log action parameters
            log_response: Log responses and errors
            log_meta: Log context metadata
            log_level: Logger level - "debug", "info", "warn", "error"
            folder: Directory for file logs (None = disabled)
            extension: File extension for logs
            whitelist: Action name patterns to log (glob patterns)

        Raises:
            ValueError: If log_level is not valid
        """
        super().__init__()

        # Validate log level
        if log_level not in ("debug", "info", "warn", "error"):
            raise ValueError(
                f"Invalid log_level: {log_level}. Must be one of: debug, info, warn, error"
            )

        self.log_params = log_params
        self.log_response = log_response
        self.log_meta = log_meta
        self.log_level: LogLevel = log_level
        self.folder = Path(folder) if folder else None
        self.extension = extension
        self.whitelist = whitelist or ["**"]

        self._logger: Logger | None = None
        self._log_fn: Callable[..., None] | None = None
        self._target_folder: Path | None = None
        self._pending_tasks: set[asyncio.Task[None]] = set()

    @property
    def name(self) -> str:
        """Middleware name."""
        return "ActionLogger"

    def broker_created(self, broker: ServiceBroker) -> None:
        """Initialize logger and create log folder.

        Called when broker is created (synchronous hook).

        Args:
            broker: Service broker instance
        """
        # Get logger from broker
        self._logger = broker.logger
        log = self._logger  # local binding — always non-None after assignment

        # Get log function by level
        if self.log_level == "debug":
            self._log_fn = log.debug
        elif self.log_level == "info":
            self._log_fn = log.info
        elif self.log_level == "warn":
            self._log_fn = log.warning
        elif self.log_level == "error":
            self._log_fn = log.error
        else:
            self._log_fn = log.info

        # Create target folder for file logs
        if self.folder:
            node_id = broker.nodeID or "unknown"
            self._target_folder = self.folder / node_id
            self._target_folder.mkdir(parents=True, exist_ok=True)
            log.info(f"ActionLogger file output: {self._target_folder}")

    def _is_whitelisted(self, action_name: str) -> bool:
        """Check if action name matches whitelist.

        Args:
            action_name: Action name (e.g., "api.v1.user.list")

        Returns:
            True if action matches any whitelist pattern
        """
        return any(match_pattern(pattern, action_name) for pattern in self.whitelist)

    def _safe_stringify(self, obj: Any) -> str:
        """Convert object to JSON string, handling special cases.

        Args:
            obj: Object to stringify

        Returns:
            JSON string
        """
        if obj is None:
            return "null"
        if obj is ...:  # Ellipsis (undefined in Python)
            return "<undefined>"

        try:
            return json.dumps(obj, default=str, indent=2)
        except (TypeError, ValueError):
            # Fallback for non-serializable objects
            return str(obj)

    async def _write_file(self, filename: str, content: str) -> None:
        """Write content to file asynchronously.

        Args:
            filename: File name
            content: File content

        Raises:
            RuntimeError: If event loop is not running
            OSError: If file write fails
        """
        if not self._target_folder:
            return

        filepath = self._target_folder / filename

        # Run blocking I/O in thread pool (Python 3.10+ compatible)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, filepath.write_text, content)

    async def _safe_write_file(self, filename: str, content: str) -> None:
        """Write file with error handling (fire-and-forget safe).

        Logs warnings on failure but doesn't raise exceptions.
        Used for background file writes that shouldn't block action execution.

        Args:
            filename: File name
            content: File content
        """
        try:
            await self._write_file(filename, content)
        except Exception as err:
            # Log warning but don't fail - file logging is optional
            if self._logger:
                self._logger.warning(f"ActionLogger failed to write log file {filename}: {err}")

    def _track_background_write(self, task: asyncio.Task[None]) -> None:
        """Track a fire-and-forget write task to avoid dangling task warnings."""
        self._pending_tasks.add(task)
        task.add_done_callback(self._on_background_write_done)

    def _on_background_write_done(self, task: asyncio.Task[None]) -> None:
        """Clean up completed write tasks and report unexpected failures."""
        self._pending_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as err:
            if self._logger:
                self._logger.warning("ActionLogger background write failed: %s", err)

    async def local_action(
        self, next_handler: Callable[..., Any], action: Any
    ) -> Callable[..., Any]:
        """Wrap local action handlers with logging.

        This hook is called during action registration, not during action execution.
        Returns a wrapped handler that logs before/after the action executes.

        Args:
            next_handler: The next handler in the middleware chain
            action: The action object being registered

        Returns:
            Wrapped handler function
        """
        action_name = action.name if hasattr(action, "name") else "unknown"

        # Whitelist filtering at registration time
        if not self._is_whitelisted(action_name):
            # Not whitelisted - return unwrapped handler
            async def passthrough_handler(ctx: Context) -> Any:
                return await next_handler(ctx)

            return passthrough_handler

        async def wrapped_handler(ctx: Context) -> Any:
            """Wrapped handler that logs action calls."""
            # 1. Log REQUEST (before execution)
            timestamp = int(time.time() * 1000)

            # Console logging
            if self._log_fn:
                msg = f"Calling '{action_name}'"
                if self.log_params:
                    msg += " with params:"
                    self._log_fn(msg, ctx.params)
                else:
                    msg += "."
                    self._log_fn(msg)

                if self.log_meta and ctx.meta:
                    self._log_fn("Meta:", ctx.meta)

            # File logging (fire-and-forget)
            if self._target_folder:
                if self.log_params:
                    filename = f"{timestamp}-call-{action_name}-request{self.extension}"
                    self._track_background_write(
                        asyncio.create_task(
                            self._safe_write_file(filename, self._safe_stringify(ctx.params))
                        )
                    )

                if self.log_meta and ctx.meta:
                    filename = f"{timestamp}-call-{action_name}-meta{self.extension}"
                    self._track_background_write(
                        asyncio.create_task(
                            self._safe_write_file(filename, self._safe_stringify(ctx.meta))
                        )
                    )

            # 2. Execute action
            try:
                result = await next_handler(ctx)

                # 3. Log RESPONSE (after successful execution)
                response_timestamp = int(time.time() * 1000)

                if self._log_fn:
                    msg = f"Response for '{action_name}' is received"
                    if self.log_response:
                        msg += ":"
                        self._log_fn(msg, result)
                    else:
                        msg += "."
                        self._log_fn(msg)

                # File logging (fire-and-forget)
                if self._target_folder and self.log_response:
                    filename = f"{response_timestamp}-call-{action_name}-response{self.extension}"
                    self._track_background_write(
                        asyncio.create_task(
                            self._safe_write_file(filename, self._safe_stringify(result))
                        )
                    )

                return result

            except Exception as err:
                # 4. Log ERROR (after failed execution)
                error_timestamp = int(time.time() * 1000)

                if self._log_fn:
                    self._log_fn(
                        f"Error for '{action_name}' is received: {err.__class__.__name__}: {err}"
                    )

                # File logging (fire-and-forget)
                if self._target_folder and self.log_response:
                    filename = f"{error_timestamp}-call-{action_name}-error{self.extension}"
                    # Serialize error with full details
                    error_data: dict[str, Any] = {
                        "type": type(err).__name__,
                        "message": str(err),
                        "traceback": traceback.format_exc(),
                        # Include all error attributes (excluding private)
                        **{k: v for k, v in err.__dict__.items() if not k.startswith("_")},
                    }
                    self._track_background_write(
                        asyncio.create_task(
                            self._safe_write_file(filename, self._safe_stringify(error_data))
                        )
                    )

                # Important: re-raise error to maintain middleware transparency
                raise

        return wrapped_handler

    async def remote_action(
        self, next_handler: Callable[..., Any], action: Any
    ) -> Callable[..., Any]:
        """Wrap remote action handlers with logging.

        Same logic as local_action - log all remote action calls.

        Args:
            next_handler: The next handler in the middleware chain
            action: The action object being registered

        Returns:
            Wrapped handler function
        """
        # Remote actions use the same logging logic as local actions
        return await self.local_action(next_handler, action)

    async def broker_stopping(self, broker: ServiceBroker) -> None:
        """Flush pending async file writes before shutdown."""
        if self._pending_tasks:
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            self._pending_tasks.clear()
