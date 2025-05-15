"""
Action Logger Middleware for the MoleculerPy framework.

This middleware logs all action calls (local and remote) for debugging
and monitoring purposes. Unlike TransitLogger which logs packets,
ActionLogger logs at the action call level with params, response, and timing.

Example:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware.action_logger import ActionLoggerMiddleware

    settings = Settings(
        middlewares=[
            ActionLoggerMiddleware(
                log_level="debug",
                log_params=True,
                log_response=False,
                whitelist=["users.*", "orders.*"]
            )
        ]
    )
    broker = ServiceBroker("node-1", settings=settings)
"""

import asyncio
import fnmatch
import json
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from .base import Middleware

logger = logging.getLogger(__name__)

# Type aliases for clarity
type ActionHandler = Callable[[Any], Awaitable[Any]]
type LogFunction = Callable[..., None]


class ActionLoggerMiddleware(Middleware):
    """Middleware that logs all action calls for debugging.

    This middleware logs incoming action requests and their responses
    via the local_action and remote_action hooks. It provides visibility
    into what actions are being called, with what parameters, and what
    they return.

    Args:
        log_level: Log level to use ("debug", "info", "warning"). Default: "info"
        log_params: Whether to log request parameters. Default: False
        log_response: Whether to log response data. Default: False
        log_meta: Whether to log context metadata. Default: False
        folder: Optional folder path to save action logs as JSON files.
        whitelist: List of action name patterns to log (fnmatch). Default: ["*"]
            Examples: ["users.*", "orders.get", "*.list"]
        blacklist: List of action name patterns to skip. Default: []

    Example:
        middleware = ActionLoggerMiddleware(
            log_level="debug",
            log_params=True,
            log_response=True,
            folder="/tmp/action-logs",
            whitelist=["users.*", "orders.*"],
            blacklist=["*.internal"]
        )
    """

    __slots__ = (
        "_log_fn",
        "_node_id",
        "_pending_tasks",
        "_target_folder",
        "blacklist",
        "folder",
        "log_level",
        "log_meta",
        "log_params",
        "log_response",
        "whitelist",
    )

    def __init__(
        self,
        log_level: str = "info",
        log_params: bool = False,
        log_response: bool = False,
        log_meta: bool = False,
        folder: str | Path | None = None,
        whitelist: list[str] | None = None,
        blacklist: list[str] | None = None,
    ) -> None:
        """Initialize the action logger middleware.

        Args:
            log_level: Logging level ("debug", "info", "warning")
            log_params: Whether to include params in logs
            log_response: Whether to include response in logs
            log_meta: Whether to include context metadata in logs
            folder: Optional folder to save action logs as JSON files
            whitelist: Action patterns to include (fnmatch)
            blacklist: Action patterns to exclude (fnmatch)
        """
        self.log_level = log_level.lower()
        self.log_params = log_params
        self.log_response = log_response
        self.log_meta = log_meta
        self.folder = Path(folder) if folder else None
        self.whitelist = whitelist or ["*"]
        self.blacklist = blacklist or []

        self._node_id: str | None = None
        self._log_fn: LogFunction | None = None
        self._target_folder: Path | None = None
        self._pending_tasks: set[asyncio.Task[None]] = set()

        # Validate log level
        level_map = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING}
        if self.log_level not in level_map:
            raise ValueError(f"Invalid log_level: {log_level}. Use: debug, info, warning")

        logger.info(
            "Action logger middleware initialized: level=%s, params=%s, response=%s",
            self.log_level,
            self.log_params,
            self.log_response,
        )

    def _should_log(self, action_name: str) -> bool:
        """Check if action should be logged based on whitelist/blacklist.

        Args:
            action_name: Name of the action

        Returns:
            True if action should be logged
        """
        # Check blacklist first
        for pattern in self.blacklist:
            if fnmatch.fnmatch(action_name, pattern):
                return False

        # Check whitelist
        for pattern in self.whitelist:
            if fnmatch.fnmatch(action_name, pattern):
                return True

        return False

    def broker_created(self, broker: Any) -> None:
        """Set up logging when broker is created.

        Args:
            broker: The broker instance
        """
        self._node_id = getattr(broker, "node_id", "unknown")

        # Set up log function based on level
        level_fn = {
            "debug": logger.debug,
            "info": logger.info,
            "warning": logger.warning,
        }
        self._log_fn = level_fn.get(self.log_level, logger.info)

        # Create output folder if specified
        if self.folder:
            node_folder = self.folder / self._node_id
            node_folder.mkdir(parents=True, exist_ok=True)
            self._target_folder = node_folder
            logger.info("Action logs will be saved to: %s", self._target_folder)
        else:
            self._target_folder = None

    def _save_to_file_sync(self, filename: str, data: dict[str, Any]) -> None:
        """Save action log to a JSON file (synchronous).

        This is the blocking I/O operation that runs in a thread pool.

        Args:
            filename: Name of the file (without folder path)
            data: Log data to save
        """
        if not self._target_folder:
            return

        try:
            filepath = self._target_folder / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.warning("Failed to save action log file: %s", e)

    async def _save_to_file_async(self, filename: str, data: dict[str, Any]) -> None:
        """Save action log to a JSON file (async, non-blocking).

        Uses asyncio.to_thread to avoid blocking the event loop.

        Args:
            filename: Name of the file (without folder path)
            data: Log data to save
        """
        if not self._target_folder:
            return

        await asyncio.to_thread(self._save_to_file_sync, filename, data)

    def _handle_task_exception(self, task: asyncio.Task[None]) -> None:
        """Handle exceptions from fire-and-forget tasks.

        Args:
            task: The completed task to check for exceptions
        """
        self._pending_tasks.discard(task)
        if not task.cancelled():
            exc = task.exception()
            if exc:
                logger.warning("Background task failed: %s", exc)

    async def local_action(self, next_handler: ActionHandler, action: Any) -> ActionHandler:
        """Wrap local action handler with logging.

        Args:
            next_handler: Next handler in the chain
            action: The action being wrapped

        Returns:
            Wrapped handler function
        """
        action_name = getattr(action, "name", "unknown")

        # Skip if not in whitelist or in blacklist
        if not self._should_log(action_name):
            return next_handler

        async def handler(ctx: Any) -> Any:
            start_time = time.monotonic()
            caller = getattr(ctx, "caller", None) or "direct"
            request_id = getattr(ctx, "id", None) or "unknown"

            # Log request
            if self._log_fn:
                self._log_fn(
                    "=> %s called by %s [%s]",
                    action_name,
                    caller,
                    request_id,
                )

                if self.log_params:
                    params = getattr(ctx, "params", {})
                    self._log_fn("   Params: %s", params)

                if self.log_meta:
                    meta = getattr(ctx, "meta", {})
                    self._log_fn("   Meta: %s", meta)

            error = None
            result = None

            try:
                result = await next_handler(ctx)
                return result
            except Exception as e:
                error = e
                raise
            finally:
                elapsed = time.monotonic() - start_time
                elapsed_ms = elapsed * 1000

                # Log response/error
                if self._log_fn:
                    if error:
                        self._log_fn(
                            "<= %s ERROR in %.2fms: %s",
                            action_name,
                            elapsed_ms,
                            str(error),
                        )
                    else:
                        self._log_fn(
                            "<= %s OK in %.2fms",
                            action_name,
                            elapsed_ms,
                        )

                        if self.log_response and result is not None:
                            self._log_fn("   Response: %s", result)

                # Save to file if configured (non-blocking)
                if self._target_folder:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                    status = "error" if error else "ok"
                    filename = f"{timestamp}-{action_name.replace('.', '_')}-{status}.json"

                    log_data = {
                        "action": action_name,
                        "caller": caller,
                        "request_id": request_id,
                        "timestamp": datetime.now().isoformat(),
                        "duration_ms": elapsed_ms,
                        "status": status,
                    }

                    if self.log_params:
                        log_data["params"] = getattr(ctx, "params", {})

                    if self.log_meta:
                        log_data["meta"] = getattr(ctx, "meta", {})

                    if error:
                        log_data["error"] = {
                            "type": type(error).__name__,
                            "message": str(error),
                        }
                    elif self.log_response and result is not None:
                        log_data["response"] = result

                    # Fire and forget with error tracking
                    task = asyncio.create_task(
                        self._save_to_file_async(filename, log_data),
                        name="moleculerpy:action-log-save",
                    )
                    self._pending_tasks.add(task)
                    task.add_done_callback(self._handle_task_exception)

        return handler

    async def remote_action(self, next_handler: ActionHandler, action: Any) -> ActionHandler:
        """Wrap remote action handler with logging.

        Uses the same logging logic as local_action.

        Args:
            next_handler: Next handler in the chain
            action: The action being wrapped

        Returns:
            Wrapped handler function
        """
        # Remote actions use the same logging pattern
        return await self.local_action(next_handler, action)

    async def stopped(self) -> None:
        """Clean up when middleware is stopped.

        Cancels any pending file save tasks and waits for completion.
        """
        # Cancel all pending tasks
        for task in self._pending_tasks:
            task.cancel()

        # Wait for cancellation to complete
        if self._pending_tasks:
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            self._pending_tasks.clear()

        logger.debug("Action logger middleware stopped")
