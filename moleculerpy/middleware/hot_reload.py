"""HotReload Middleware for the MoleculerPy framework.

This module implements hot reloading of services during development.
It watches service files for changes and automatically reloads them
without restarting the entire broker.

Features:
    - File watching for service modules
    - Automatic service reload on file changes
    - Debounced reload to batch multiple file changes
    - Graceful service restart (stop → reload → start)
    - Dependency tracking for related modules

Requirements:
    - watchdog library (optional, for file watching)
    - Development use only (not recommended for production)

Example usage:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware import HotReloadMiddleware

    settings = Settings(
        middlewares=[
            HotReloadMiddleware(
                watch_paths=["./services"],
                debounce_ms=500,
            ),
        ],
    )

    broker = ServiceBroker(id="dev-node", settings=settings)

Configuration options:
    watch_paths: List of directories to watch for changes
    debounce_ms: Time to wait for additional changes before reload (default: 500)
    patterns: File patterns to watch (default: ["*.py"])
    recursive: Watch subdirectories (default: True)

Moleculer.js compatible concepts:
    - broker.options.hotReload (enabled via middleware)
    - Debounced reload (500ms default)
    - Service dependency tracking

Note:
    This middleware is designed for development use only.
    For production, use process managers like systemd or supervisord.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from moleculerpy.middleware.base import Middleware

if TYPE_CHECKING:
    from moleculerpy.broker import ServiceBroker
    from moleculerpy.service import Service

__all__ = ["HotReloadMiddleware"]


class HotReloadMiddleware(Middleware):
    """Middleware for hot reloading services during development.

    Watches service files for changes and automatically reloads them.
    Uses debouncing to batch multiple file changes into a single reload.

    This is intended for development use only. In production, use
    proper process managers for service restarts.

    Attributes:
        logger: Logger for reload events
        watch_paths: List of directories to watch
        debounce_ms: Debounce time in milliseconds
        patterns: File patterns to watch
        recursive: Whether to watch subdirectories
        _broker: Reference to the broker
        _observer: File system observer (if watchdog available)
        _pending_reloads: Set of services pending reload
        _reload_task: Debounced reload task handle
    """

    __slots__ = (
        "_background_tasks",
        "_broker",
        "_observer",
        "_pending_reloads",
        "_reload_task",
        "_service_modules",
        "debounce_ms",
        "logger",
        "patterns",
        "recursive",
        "watch_paths",
    )

    def __init__(
        self,
        watch_paths: list[str] | None = None,
        debounce_ms: int = 500,
        patterns: list[str] | None = None,
        recursive: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize the HotReloadMiddleware.

        Args:
            watch_paths: List of directories to watch for changes
            debounce_ms: Time to wait for additional changes before reload
            patterns: File patterns to watch (default: ["*.py"])
            recursive: Watch subdirectories (default: True)
            logger: Optional logger for reload events
        """
        super().__init__()
        self.logger = logger or logging.getLogger("moleculerpy.middleware.hot_reload")
        self.watch_paths = watch_paths or ["./services"]
        self.debounce_ms = debounce_ms
        self.patterns = patterns or ["*.py"]
        self.recursive = recursive
        self._broker: ServiceBroker | None = None
        self._observer: Any = None
        self._pending_reloads: set[str] = set()
        self._reload_task: asyncio.TimerHandle | None = None
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._service_modules: dict[str, str] = {}  # service_name -> module_name

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"HotReloadMiddleware(paths={self.watch_paths}, debounce={self.debounce_ms}ms)"

    def _get_module_for_service(self, service: Service) -> str | None:
        """Get the module name for a service.

        Args:
            service: Service instance

        Returns:
            Module name or None if not found
        """
        # Try to get module from service class
        service_class = type(service)
        module_name = service_class.__module__

        if module_name and module_name != "__main__":
            return module_name

        return None

    def _reload_module(self, module_name: str) -> bool:
        """Reload a Python module.

        Args:
            module_name: Name of the module to reload

        Returns:
            True if reload succeeded, False otherwise
        """
        try:
            if module_name in sys.modules:
                module = sys.modules[module_name]
                importlib.reload(module)
                self.logger.info(f"Reloaded module: {module_name}")
                return True
            else:
                self.logger.warning(f"Module not loaded: {module_name}")
                return False
        except Exception as err:
            self.logger.error(f"Failed to reload module {module_name}: {err}")
            return False

    async def _reload_service(self, service_name: str) -> bool:
        """Reload a single service.

        Stops the service, reloads its module, and restarts it.

        Args:
            service_name: Name of the service to reload

        Returns:
            True if reload succeeded, False otherwise
        """
        if self._broker is None:
            return False

        # Find the service
        service = None
        broker_services = getattr(self._broker, "services", None)
        if broker_services is None:
            broker_services = list(getattr(self._broker.registry, "__services__", {}).values())
        for svc in broker_services:
            if service_name in (svc.name, getattr(svc, "full_name", svc.name)):
                service = svc
                break

        if service is None:
            self.logger.warning(f"Service not found: {service_name}")
            return False

        module_name = self._service_modules.get(service_name)
        if not module_name:
            module_name = self._get_module_for_service(service)

        if not module_name:
            self.logger.warning(f"Cannot determine module for service: {service_name}")
            return False

        try:
            # Stop the service
            self.logger.info(f"Stopping service '{service_name}' for hot reload...")
            if hasattr(service, "_call_lifecycle_hook"):
                await service._call_lifecycle_hook("stopped")

            # Reload the module
            if not self._reload_module(module_name):
                return False

            # Get the updated service class
            module = sys.modules.get(module_name)
            if module is None:
                self.logger.error(f"Module not found after reload: {module_name}")
                return False

            # Find and instantiate the new service class
            service_class = None
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and hasattr(attr, "name")
                    and getattr(attr, "name", None) == service_name
                ):
                    service_class = attr
                    break

            if service_class is None:
                self.logger.error(f"Service class not found in module: {module_name}")
                return False

            # Create new service instance
            new_service = service_class(self._broker)

            # Register and start the new service
            await self._broker.register(new_service)
            if hasattr(new_service, "_call_lifecycle_hook"):
                await new_service._call_lifecycle_hook("started")

            self.logger.info(f"Service '{service_name}' hot reloaded successfully")
            return True

        except Exception as err:
            self.logger.error(f"Hot reload failed for '{service_name}': {err}")
            return False

    async def _process_pending_reloads(self) -> None:
        """Process all pending service reloads."""
        self._reload_task = None

        if not self._pending_reloads:
            return

        services_to_reload = self._pending_reloads.copy()
        self._pending_reloads.clear()

        self.logger.info(f"Hot reloading {len(services_to_reload)} service(s)...")

        for service_name in services_to_reload:
            await self._reload_service(service_name)

    def _schedule_reload(self, service_name: str) -> None:
        """Schedule a debounced reload for a service.

        Args:
            service_name: Name of the service to reload
        """
        self._pending_reloads.add(service_name)

        # Cancel existing reload task
        if self._reload_task is not None:
            self._reload_task.cancel()

        # Schedule new reload
        debounce_sec = self.debounce_ms / 1000.0

        def do_reload() -> None:
            task = asyncio.create_task(self._process_pending_reloads())
            self._background_tasks.add(task)
            task.add_done_callback(self._on_background_task_done)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Fallback for edge cases where no loop is running
            loop = asyncio.new_event_loop()
        self._reload_task = loop.call_later(debounce_sec, do_reload)

        self.logger.debug(f"Scheduled reload for '{service_name}' in {self.debounce_ms}ms")

    def _on_background_task_done(self, task: asyncio.Task[None]) -> None:
        """Remove completed background task and log any unexpected errors."""
        self._background_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as err:  # pragma: no cover - defensive logging
            self.logger.error("Hot reload background task failed: %s", err)

    def _on_file_changed(self, file_path: str) -> None:
        """Handle file change event.

        Args:
            file_path: Path to the changed file
        """
        path = Path(file_path)

        # Check if file matches our patterns
        if not any(path.match(pattern) for pattern in self.patterns):
            return

        self.logger.debug(f"File changed: {file_path}")

        # Find services that depend on this file
        for service_name, module_name in self._service_modules.items():
            # Simple check: if module path matches file path
            module = sys.modules.get(module_name)
            if module and hasattr(module, "__file__"):
                module_file = getattr(module, "__file__", None)
                if module_file:
                    module_path = Path(module_file).resolve()
                    if module_path == path.resolve():
                        self._schedule_reload(service_name)
                        return

        # If no specific service found, try to reload all services in the path
        self.logger.debug(f"No specific service found for {file_path}")

    def _start_watching(self) -> None:
        """Start file system watching."""
        try:
            from watchdog.events import FileSystemEventHandler  # noqa: PLC0415
            from watchdog.observers import Observer  # noqa: PLC0415

            class HotReloadHandler(FileSystemEventHandler):
                def __init__(self, middleware: HotReloadMiddleware):
                    super().__init__()
                    self.middleware = middleware

                def on_modified(self, event: Any) -> None:
                    if not event.is_directory:
                        self.middleware._on_file_changed(event.src_path)

                def on_created(self, event: Any) -> None:
                    if not event.is_directory:
                        self.middleware._on_file_changed(event.src_path)

            self._observer = Observer()
            handler = HotReloadHandler(self)

            for watch_path in self.watch_paths:
                path = Path(watch_path)
                if path.exists() and path.is_dir():
                    self._observer.schedule(handler, str(path), recursive=self.recursive)
                    self.logger.info(f"Watching for changes: {path}")
                else:
                    self.logger.warning(f"Watch path not found: {watch_path}")

            self._observer.start()
            self.logger.info("Hot reload file watching started")

        except ImportError:
            self.logger.warning(
                "watchdog not installed. Hot reload disabled. Install with: pip install watchdog"
            )

    def _stop_watching(self) -> None:
        """Stop file system watching."""
        if self._observer is not None:
            self._observer.stop()
            self._observer.join()
            self._observer = None
            self.logger.info("Hot reload file watching stopped")

    # ========== Lifecycle Hooks ==========

    def broker_created(self, broker: ServiceBroker) -> None:
        """Store broker reference.

        Args:
            broker: The broker instance
        """
        self._broker = broker
        self.logger.debug("HotReload middleware attached to broker")

    async def service_started(self, service: Service) -> None:
        """Track service module for hot reload.

        Args:
            service: The started service
        """
        module_name = self._get_module_for_service(service)
        if module_name:
            self._service_modules[service.name] = module_name
            self.logger.debug(f"Tracking service '{service.name}' from {module_name}")

    async def broker_started(self, broker: ServiceBroker) -> None:
        """Start file watching when broker starts.

        Args:
            broker: The broker instance
        """
        self._start_watching()

    async def broker_stopping(self, broker: ServiceBroker) -> None:
        """Stop file watching when broker stops.

        Args:
            broker: The broker instance
        """
        self._stop_watching()

        # Cancel any pending reload
        if self._reload_task is not None:
            self._reload_task.cancel()
            self._reload_task = None

        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()
