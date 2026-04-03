"""Base Service class for the MoleculerPy framework.

This module provides the base Service class that all microservices should inherit from.
It provides functionality for discovering actions and events defined in the service,
as well as lifecycle hooks for initialization and cleanup.

Mixin Support:
    Services can use mixins to compose reusable functionality:

    class LoggerMixin:
        __settings__ = {"log_level": "debug"}

        def log(self, message: str):
            self.logger.info(message)

    class UserService(Service):
        name = "users"
        mixins = [LoggerMixin]

        @action()
        async def get_user(self, ctx):
            self.log("Getting user")  # From mixin
            return {"id": 1}
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from typing import Any, ClassVar, cast

from moleculerpy.mixin import (
    HooksSchema,
    Mixin,
    MixinSchema,
    apply_mixins,
    extract_mixin_methods,
    merge_settings,
)


class Service:
    """Base class for all MoleculerPy microservices.

    Services should inherit from this class and define their actions and events
    as methods decorated with @action or @event decorators.

    Class Attributes:
        mixins: List of mixin classes or dicts to compose into this service.
                Mixins are processed in order, with later mixins and the
                service class itself having higher priority.

    Lifecycle hooks:
        Services can define the following async methods to hook into their lifecycle:

        - async def created(self): Called after service is registered with the broker
        - async def started(self): Called after the broker is fully started
        - async def stopped(self): Called when the broker is stopping

    Example:
        # Define a mixin
        class LoggerMixin:
            __settings__ = {"log_level": "debug"}

            def log(self, message: str):
                self.logger.info(message)

        # Use mixin in service
        class UserService(Service):
            name = "users"
            mixins = [LoggerMixin]
            settings = {"cache_ttl": 300}  # Merged with mixin settings

            def __init__(self):
                super().__init__()
                self.db = None

            async def created(self):
                '''Called after registration - initialize resources.'''
                self.db = await connect_database()
                self.log("Database connected")  # Method from mixin!

            async def started(self):
                '''Called when broker is ready - warm up caches.'''
                await self.warmup_cache()

            async def stopped(self):
                '''Called on shutdown - cleanup resources.'''
                if self.db:
                    await self.db.close()
    """

    # Class-level attributes for schema definition (read via __dict__ in __init_subclass__)
    # Users define these on subclasses, they are NOT inherited as values
    # name: str - service name
    # version: str - service version
    # mixins: list - list of mixin classes/dicts
    # settings: dict - service settings (merged with mixins)
    # metadata: dict - service metadata (merged with mixins)
    # dependencies: list - service dependencies (merged with mixins)
    # hooks: dict - action hooks

    # Class-level storage for merged mixin data (set by __init_subclass__)
    _merged_schema: ClassVar[MixinSchema] = {}
    _lifecycle_handlers: ClassVar[dict[str, list[Callable[..., Any]]]] = {}
    _class_settings: ClassVar[dict[str, Any]] = {}
    _class_metadata: ClassVar[dict[str, Any]] = {}
    _class_dependencies: ClassVar[list[str]] = []
    hooks: ClassVar[dict[str, Any]] = {}
    mixins: ClassVar[Sequence[Mixin]] = []

    __slots__ = (
        "_extra",
        "broker",
        "dependencies",
        "full_name",
        "logger",
        "metadata",
        "name",
        "settings",
        "version",
    )

    @staticmethod
    def get_versioned_full_name(name: str, version: int | str | None) -> str:
        """Build a versioned full service name.

        Follows Moleculer.js convention (service.js:835):
        - Numeric version 2 -> "v2.users"
        - String version "staging" -> "staging.users"
        - None -> "users" (unchanged)

        Args:
            name: Service name (e.g. "users")
            version: Version number or string, or None

        Returns:
            Versioned full name (e.g. "v2.users") or plain name
        """
        if version is not None and version != "":
            prefix = f"v{version}" if isinstance(version, int) else str(version)
            return f"{prefix}.{name}"
        return name

    def __setattr__(self, name: str, value: Any) -> None:
        """Allow dynamic service state while keeping a fixed-slot core layout."""
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return

        try:
            extra = object.__getattribute__(self, "_extra")
        except AttributeError:
            extra = None
        if extra is None:
            extra = {}
            object.__setattr__(self, "_extra", extra)
        extra[name] = value

    def __getattr__(self, name: str) -> Any:
        """Resolve dynamic attributes stored in the extension map."""
        try:
            extra = object.__getattribute__(self, "_extra")
        except AttributeError:
            extra = None
        if extra is not None and name in extra:
            return extra[name]
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Process mixins when a subclass is defined.

        This method is called automatically when a class inherits from Service.
        It merges all mixins and prepares the combined schema.
        """
        super().__init_subclass__(**kwargs)

        # Skip base Service class
        if cls.__name__ == "Service":
            return

        # Build schema from class attributes
        # Use __dict__ to avoid getting property descriptors from parent class
        class_schema: MixinSchema = {}

        # name and version are simple strings
        name = cls.__dict__.get("name")
        if isinstance(name, str) and name:
            class_schema["name"] = name

        version = cls.__dict__.get("version")
        if isinstance(version, int) or (isinstance(version, str) and version):
            class_schema["version"] = version

        # settings, metadata, hooks must be dicts
        settings = cls.__dict__.get("settings")
        if isinstance(settings, dict) and settings:
            class_schema["settings"] = settings

        metadata = cls.__dict__.get("metadata")
        if isinstance(metadata, dict) and metadata:
            class_schema["metadata"] = metadata

        hooks = cls.__dict__.get("hooks")
        if isinstance(hooks, dict) and hooks:
            class_schema["hooks"] = cast(HooksSchema, hooks)

        # dependencies must be list
        dependencies = cls.__dict__.get("dependencies")
        if isinstance(dependencies, list) and dependencies:
            class_schema["dependencies"] = dependencies

        # Get mixins from class
        mixins = getattr(cls, "mixins", [])

        if mixins:
            # Apply mixins to get merged schema
            cls._merged_schema = apply_mixins(class_schema, mixins)

            # Extract lifecycle handlers
            cls._lifecycle_handlers = {}
            for hook_name in ("created", "started", "stopped", "merged"):
                handlers = cls._merged_schema.get(hook_name)
                if handlers:
                    if isinstance(handlers, list):
                        cls._lifecycle_handlers[hook_name] = [
                            handler for handler in handlers if callable(handler)
                        ]
                    elif callable(handlers):
                        cls._lifecycle_handlers[hook_name] = [handlers]

            # Apply merged values to class-level attributes
            # This allows `svc.settings` to work without going through property
            if "settings" in cls._merged_schema:
                cls._class_settings = cls._merged_schema["settings"]

            if "metadata" in cls._merged_schema:
                cls._class_metadata = cls._merged_schema["metadata"]

            if "dependencies" in cls._merged_schema:
                cls._class_dependencies = cls._merged_schema["dependencies"]

            if "hooks" in cls._merged_schema:
                cls.hooks = cast(dict[str, Any], cls._merged_schema["hooks"])
        else:
            cls._merged_schema = class_schema
            cls._lifecycle_handlers = {}

            # Propagate class-level attributes even without mixins
            if "settings" in class_schema:
                cls._class_settings = class_schema["settings"]
            if "metadata" in class_schema:
                cls._class_metadata = class_schema["metadata"]
            if "dependencies" in class_schema:
                cls._class_dependencies = class_schema["dependencies"]

    def __init__(
        self,
        name: str | None = None,
        settings: dict[str, Any] | None = None,
        dependencies: list[str] | None = None,
    ) -> None:
        """Initialize a new Service instance.

        Args:
            name: The name of the service (may include version).
                  If not provided, uses class attribute.
            settings: Optional service-specific settings.
                      Merged with class-level settings and mixin settings.
            dependencies: List of service names this service depends on.
                         The broker will wait for these services before calling started().
                         Merged with class-level and mixin dependencies.
        """
        cls = self.__class__

        # Service name: instance arg > class attribute > class name
        class_name = cls.__dict__.get("name", "")
        if not isinstance(class_name, str):
            class_name = ""
        self.name: str = name or class_name or cls.__name__.lower()

        # Version: from merged schema (supports int and str)
        merged_schema = getattr(cls, "_merged_schema", {})
        raw_version = merged_schema.get("version") if isinstance(merged_schema, dict) else None
        self.version: int | str | None = raw_version

        # Full name: versioned service name (e.g. "v2.users")
        # Respects $noVersionPrefix setting (checked after settings merge below)
        # Will be finalized after settings are merged
        self.full_name: str = self.name  # Temporary, finalized below

        # Settings: merge class-level (from mixins) + instance arg
        # Use _class_settings which contains merged mixin + class settings
        class_settings = getattr(cls, "_class_settings", None)
        if class_settings is None:
            # No mixins - use class __dict__ directly
            raw = cls.__dict__.get("settings")
            class_settings = raw if isinstance(raw, dict) else {}
        self.settings: dict[str, Any] = merge_settings(class_settings, settings or {})

        # Finalize full_name now that settings are available
        # Node.js: settings.$noVersionPrefix suppresses version prefix
        effective_version = (
            self.version if not self.settings.get("$noVersionPrefix", False) else None
        )
        self.full_name = Service.get_versioned_full_name(self.name, effective_version)

        # Metadata: from class-level (already merged in __init_subclass__)
        class_metadata = getattr(cls, "_class_metadata", None)
        if class_metadata is None:
            raw = cls.__dict__.get("metadata")
            class_metadata = raw if isinstance(raw, dict) else {}
        self.metadata: dict[str, Any] = dict(class_metadata)

        # Dependencies: merge class-level + instance arg (unique, preserve order)
        class_deps = getattr(cls, "_class_dependencies", None)
        if class_deps is None:
            raw = cls.__dict__.get("dependencies")
            class_deps = raw if isinstance(raw, list) else []
        instance_deps = dependencies or []
        seen: set[str] = set()
        merged_deps: list[str] = []
        for dep in list(class_deps) + instance_deps:
            if dep not in seen:
                seen.add(dep)
                merged_deps.append(dep)
        self.dependencies: list[str] = merged_deps

        # Instance attributes for broker integration
        self._extra: dict[str, Any] | None = None
        self.broker: Any | None = None
        self.logger: Any | None = None

        # Apply mixin methods to instance
        merged_schema = getattr(cls, "_merged_schema", {})
        if isinstance(merged_schema, dict) and merged_schema:
            extract_mixin_methods(self, cast(MixinSchema, merged_schema))

    def actions(self) -> list[str]:
        """Get a list of all action method names in this service.

        Returns:
            List of method names that are marked as actions
        """
        result: list[str] = []
        for attr in dir(self):
            obj = getattr(self, attr, None)
            if obj is not None and callable(obj) and getattr(obj, "_is_action", False):
                result.append(attr)
        return result

    def events(self) -> list[str]:
        """Get a list of all event handler method names in this service.

        Returns:
            List of method names that are marked as event handlers
        """
        result: list[str] = []
        for attr in dir(self):
            obj = getattr(self, attr, None)
            if obj is not None and callable(obj) and getattr(obj, "_is_event", False):
                result.append(attr)
        return result

    def has_lifecycle_hook(self, hook_name: str) -> bool:
        """Check if this service has a specific lifecycle hook defined.

        Args:
            hook_name: Name of the hook (created, started, stopped)

        Returns:
            True if the hook method exists and is callable
        """
        # Check for mixin lifecycle handlers
        lifecycle_handlers = getattr(self.__class__, "_lifecycle_handlers", {})
        if lifecycle_handlers.get(hook_name):
            return True

        hook = getattr(self, hook_name, None)
        # Check if it's a method defined on the subclass (not base class)
        if hook is None:
            return False
        # Check if it's callable and defined in a subclass
        if not callable(hook):
            return False
        # Check if it's not just the base class placeholder
        base_hook = getattr(Service, hook_name, None)
        return hook is not base_hook or hook_name not in ("created", "started", "stopped")

    async def _call_lifecycle_hook(self, hook_name: str) -> None:
        """Call a lifecycle hook if it exists.

        Calls both mixin lifecycle handlers and the service's own hook method.

        For 'created', 'started', 'merged':
            Mixin handlers are called first, then service's own hook.
            Order: mixin1 → mixin2 → service

        For 'stopped':
            Order is REVERSED - service's hook first, then mixin handlers in reverse.
            Order: service → mixin2 → mixin1

        This matches Moleculer.js behavior where cleanup happens in reverse
        initialization order.

        Args:
            hook_name: Name of the hook to call
        """
        lifecycle_handlers = getattr(self.__class__, "_lifecycle_handlers", {})
        handlers_list = lifecycle_handlers.get(hook_name, [])

        # Get service's own hook
        service_hook = getattr(self, hook_name, None)
        base_hook = getattr(Service, hook_name, None)
        has_service_hook = service_hook and callable(service_hook) and service_hook is not base_hook

        async def call_handler(handler: Any) -> None:
            """Call a single handler, handling both sync and async."""
            if callable(handler):
                try:
                    result = handler(self)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Error in {hook_name} handler: {e}")
                    raise

        async def call_service_hook() -> None:
            """Call service's own hook."""
            if has_service_hook:
                service_callable = cast(Callable[[], Any], service_hook)
                result = service_callable()
                if asyncio.iscoroutine(result):
                    await result

        match hook_name:
            case "stopped":
                # REVERSE order for stopped: service first, then mixins in reverse
                await call_service_hook()
                for handler in reversed(handlers_list):
                    await call_handler(handler)
            case _:
                # Normal order for created, started, merged: mixins first, then service
                for handler in handlers_list:
                    await call_handler(handler)
                await call_service_hook()

    def get_schema(self) -> MixinSchema:
        """Get the merged schema for this service.

        Returns:
            The schema with all mixins applied
        """
        schema = getattr(self.__class__, "_merged_schema", {})
        return cast(MixinSchema, schema)

    def get_hooks(self) -> dict[str, Any]:
        """Get the merged hooks for this service.

        Returns:
            Hooks dictionary with all mixin hooks merged
        """
        return getattr(self.__class__, "hooks", {}) or {}
