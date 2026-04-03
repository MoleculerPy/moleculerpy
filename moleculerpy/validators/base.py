"""Base validator interface for MoleculerPy pluggable validator system.

Follows the Moleculer.js pattern from src/validators/base.js.
The validator integrates into the broker as a middleware via middleware().
compile() is called once per action/event registration; the checker runs on each call.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from moleculerpy.broker import ServiceBroker

CheckFunction = Callable[[dict[str, Any]], bool]


class BaseValidator(ABC):
    """Abstract base class for all validators.

    Node.js equivalent: src/validators/base.js

    Implement compile() and validate() to create a custom validator.
    Pass it as ``Settings(validator=MyValidator())`` to ServiceBroker.
    """

    broker: ServiceBroker | None
    logger: Any

    def __init__(self) -> None:
        self.broker = None
        self.logger = None

    def init(self, broker: ServiceBroker) -> None:
        """Called by broker at startup.

        Args:
            broker: The ServiceBroker instance.
        """
        self.broker = broker
        self.logger = getattr(broker, "logger", None)

    @abstractmethod
    def compile(self, schema: dict[str, Any]) -> CheckFunction:
        """Compile a schema into a reusable check function.

        Called once per action/event registration so the result is cached.
        The returned checker is called on each request/event.

        Args:
            schema: Validation schema dict.

        Returns:
            A callable that accepts params dict and returns True on success
            or raises ValidationError on failure.
        """
        ...

    @abstractmethod
    def validate(self, params: dict[str, Any], schema: dict[str, Any]) -> bool:
        """Validate params against schema (convenience, calls compile internally).

        Args:
            params: The parameters to validate.
            schema: The validation schema dict.

        Returns:
            True if validation passes.

        Raises:
            ValidationError: If validation fails.
        """
        ...

    def convert_schema_to_moleculer(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Convert native schema format to Moleculer standard for documentation.

        Node.js equivalent: convertSchemaToMoleculer()
        Default implementation passes through unchanged.

        Args:
            schema: Native validator schema.

        Returns:
            Moleculer-compatible schema dict.
        """
        return schema

    def middleware(self) -> dict[str, Any]:
        """Return middleware hooks for action and event validation.

        Node.js equivalent: BaseValidator.middleware(broker)
        The broker registers this middleware automatically if a validator is set.

        Returns:
            Dict with 'local_action' and 'local_event' async hooks.
        """
        validator = self

        async def local_action(
            next_handler: Callable[..., Awaitable[Any]],
            action: Any,
        ) -> Callable[..., Awaitable[Any]]:
            """Wrap action handler with param validation.

            compile() is called once here (at registration time).
            The checker runs on each action call.
            """
            params_schema = getattr(action, "params_schema", None) or getattr(
                action, "params", None
            )
            if not params_schema:
                return next_handler

            # Compile once at registration — cache the checker
            checker = validator.compile(params_schema)

            async def validate_handler(ctx: Any) -> Any:
                params = ctx.params if ctx.params is not None else {}
                checker(params)
                return await next_handler(ctx)

            return validate_handler

        async def local_event(
            next_handler: Callable[..., Awaitable[Any]],
            event: Any,
        ) -> Callable[..., Awaitable[Any]]:
            """Wrap event handler with param validation.

            Node.js validates event params too — same pattern as actions.
            """
            params_schema = getattr(event, "params_schema", None) or getattr(event, "params", None)
            if not params_schema:
                return next_handler

            checker = validator.compile(params_schema)

            async def validate_handler(ctx: Any) -> Any:
                params = ctx.params if ctx.params is not None else {}
                checker(params)
                return await next_handler(ctx)

            return validate_handler

        return {
            "local_action": local_action,
            "local_event": local_event,
        }


__all__ = ["BaseValidator", "CheckFunction"]
