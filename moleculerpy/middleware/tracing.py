"""Tracing middleware for MoleculerPy framework.

Provides automatic distributed tracing for service actions and events:
- Span creation for action calls (local and remote)
- Error tracking and propagation
- Context propagation through call chains
- Pluggable exporters (Console, Event, Jaeger, Zipkin)

Based on Moleculer.js tracing middleware design patterns.

Usage:
    from moleculerpy.middleware import TracingMiddleware
    from moleculerpy.tracing import ConsoleExporter

    tracing = TracingMiddleware({
        'enabled': True,
        'exporter': [ConsoleExporter()],
        'sampling': {'rate': 1.0},
    })

    settings = Settings(middlewares=[tracing])
    broker = ServiceBroker("my-node", settings=settings)
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from moleculerpy.middleware.base import Middleware
from moleculerpy.tracing import Tracer, TracerOptions

T = TypeVar("T")
HandlerType = Callable[[Any], Awaitable[T]]


class TracingMiddleware(Middleware):
    """Middleware for distributed tracing.

    Automatically creates spans for action calls and optionally events.
    Integrates with the broker's tracer to export spans to backends.

    Attributes:
        opts: Tracing configuration options
        tracer: Tracer instance (set during broker_created)

    Options:
        enabled: Enable/disable tracing (default: True)
        exporter: List of exporters or single exporter
        sampling_rate: Probabilistic sampling rate 0.0-1.0 (default: 1.0)
        sampling_traces_per_second: Rate-limiting (max traces/sec)
        sampling_min_priority: Minimum priority to sample
        actions: Trace action calls (default: True)
        events: Trace event handlers (default: False)
        default_tags: Default tags for all spans

    Example:
        from moleculerpy.middleware import TracingMiddleware
        from moleculerpy.tracing import ConsoleExporter, EventExporter

        # Development: print to console
        tracing = TracingMiddleware({
            'enabled': True,
            'exporter': [ConsoleExporter({'colors': True})],
        })

        # Production: emit events for collection
        tracing = TracingMiddleware({
            'enabled': True,
            'exporter': [EventExporter()],
            'sampling': {'rate': 0.1},  # 10% sampling
        })
    """

    __slots__ = ("_broker", "opts", "tracer")

    def __init__(self, opts: dict[str, Any] | TracerOptions | None = None) -> None:
        """Initialize tracing middleware.

        Args:
            opts: Tracing configuration options (dict or TracerOptions)
        """
        super().__init__()

        # Parse options
        if opts is None:
            self.opts = TracerOptions()
        elif isinstance(opts, TracerOptions):
            self.opts = opts
        else:
            # Handle nested 'sampling' key from Moleculer.js config style
            flat_opts = self._flatten_opts(opts)
            self.opts = TracerOptions(**flat_opts)

        self.tracer: Tracer | None = None
        self._broker: Any = None

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return (
            f"TracingMiddleware(enabled={self.opts.enabled}, "
            f"actions={self.opts.actions}, events={self.opts.events})"
        )

    def _flatten_opts(self, opts: dict[str, Any]) -> dict[str, Any]:
        """Flatten Moleculer.js style options to TracerOptions format.

        Converts:
            {'sampling': {'rate': 0.5}} -> {'sampling_rate': 0.5}

        Args:
            opts: Nested options dict

        Returns:
            Flattened options dict
        """
        result = {}

        for key, value in opts.items():
            if key == "sampling" and isinstance(value, dict):
                # Flatten sampling sub-options
                if "rate" in value:
                    result["sampling_rate"] = value["rate"]
                if "tracesPerSecond" in value:
                    result["sampling_traces_per_second"] = value["tracesPerSecond"]
                if "traces_per_second" in value:
                    result["sampling_traces_per_second"] = value["traces_per_second"]
                if "minPriority" in value:
                    result["sampling_min_priority"] = value["minPriority"]
                if "min_priority" in value:
                    result["sampling_min_priority"] = value["min_priority"]
            else:
                result[key] = value

        return result

    def broker_created(self, broker: Any) -> None:
        """Initialize tracer when broker is created.

        Args:
            broker: The broker instance
        """
        self._broker = broker

        # Create tracer instance
        self.tracer = Tracer(broker, self.opts)

        # Attach tracer to broker for context.start_span() access
        broker.tracer = self.tracer

    async def broker_starting(self, broker: Any) -> None:
        """Initialize tracer exporters when broker starts.

        Args:
            broker: The broker instance
        """
        if self.tracer:
            self.tracer.init()

    async def broker_stopped(self, broker: Any) -> None:
        """Stop tracer when broker stops.

        Args:
            broker: The broker instance
        """
        if self.tracer:
            self.tracer.stop()

    async def local_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Wrap local action with tracing span.

        Args:
            next_handler: The next handler in the chain
            action: The action object being registered

        Returns:
            Wrapped handler with tracing
        """
        if not self.opts.enabled or not self.opts.actions:
            return next_handler

        action_name = getattr(action, "name", str(action))

        async def traced_handler(ctx: Any) -> Any:
            # Check if tracing is disabled for this context
            if ctx.tracing is False:
                return await next_handler(ctx)

            # Skip if tracer not available
            if not self.tracer or not self.tracer.is_enabled():
                return await next_handler(ctx)

            # Build span options
            span_opts = self._build_action_span_opts(ctx, action, "local")

            # Create and start span
            span = self.tracer.start_span(action_name, span_opts)

            # Attach span to context
            ctx.span = span

            try:
                result = await next_handler(ctx)
                return result
            except asyncio.CancelledError:
                span.add_tags({"status": "cancelled"})
                raise
            except Exception as e:
                span.set_error(e)
                raise
            finally:
                span.finish()

        return traced_handler

    async def remote_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Wrap remote action call with tracing span.

        Args:
            next_handler: The next handler in the chain
            action: The action object being registered

        Returns:
            Wrapped handler with tracing
        """
        if not self.opts.enabled or not self.opts.actions:
            return next_handler

        action_name = getattr(action, "name", str(action))

        async def traced_handler(ctx: Any) -> Any:
            # Check if tracing is disabled for this context
            if ctx.tracing is False:
                return await next_handler(ctx)

            # Skip if tracer not available
            if not self.tracer or not self.tracer.is_enabled():
                return await next_handler(ctx)

            # Build span options
            span_opts = self._build_action_span_opts(ctx, action, "remote")

            # Create and start span
            span = self.tracer.start_span(action_name, span_opts)

            # Attach span to context
            ctx.span = span

            try:
                result = await next_handler(ctx)
                return result
            except asyncio.CancelledError:
                span.add_tags({"status": "cancelled"})
                raise
            except Exception as e:
                span.set_error(e)
                raise
            finally:
                span.finish()

        return traced_handler

    async def local_event(self, next_handler: HandlerType[Any], event: Any) -> HandlerType[Any]:
        """Wrap local event handler with tracing span.

        Args:
            next_handler: The next handler in the chain
            event: The event object being registered

        Returns:
            Wrapped handler with tracing (if events tracing enabled)
        """
        if not self.opts.enabled or not self.opts.events:
            return next_handler

        event_name = getattr(event, "name", str(event))

        async def traced_handler(ctx: Any) -> Any:
            # Check if tracing is disabled for this context
            if ctx.tracing is False:
                return await next_handler(ctx)

            # Skip if tracer not available
            if not self.tracer or not self.tracer.is_enabled():
                return await next_handler(ctx)

            # Build span options
            span_opts = self._build_event_span_opts(ctx, event)

            # Create and start span
            span = self.tracer.start_span(event_name, span_opts)

            # Attach span to context
            ctx.span = span

            try:
                result = await next_handler(ctx)
                return result
            except asyncio.CancelledError:
                span.add_tags({"status": "cancelled"})
                raise
            except Exception as e:
                span.set_error(e)
                raise
            finally:
                span.finish()

        return traced_handler

    def _build_action_span_opts(
        self,
        ctx: Any,
        action: Any,
        action_type: str,
    ) -> dict[str, Any]:
        """Build span options for action call.

        Args:
            ctx: Request context
            action: Action object
            action_type: "local" or "remote"

        Returns:
            Span options dict
        """
        opts: dict[str, Any] = {
            "type": "action",
            "trace_id": ctx.request_id,
        }

        # Parent span
        if ctx.parent_id:
            opts["parent_id"] = ctx.parent_id

        # Service info
        service = getattr(ctx, "service", None)
        if service:
            opts["service"] = {
                "name": getattr(service, "name", None),
                "version": getattr(service, "version", None),
                "node_id": ctx.node_id,
            }

        # Tags
        action_name = getattr(action, "name", str(action))
        opts["tags"] = {
            "action": action_name,
            "action_type": action_type,
            "caller": ctx.caller,
            "node_id": ctx.node_id,
            "level": ctx.level,
        }

        # Add params summary (avoid large payloads)
        if ctx.params:
            param_keys = list(ctx.params.keys())[:10]  # Max 10 keys
            opts["tags"]["params_keys"] = param_keys

        return opts

    def _build_event_span_opts(
        self,
        ctx: Any,
        event: Any,
    ) -> dict[str, Any]:
        """Build span options for event handler.

        Args:
            ctx: Event context
            event: Event object

        Returns:
            Span options dict
        """
        opts: dict[str, Any] = {
            "type": "event",
            "trace_id": ctx.request_id,
        }

        # Parent span (if event is part of a call chain)
        if ctx.parent_id:
            opts["parent_id"] = ctx.parent_id

        # Service info
        service = getattr(ctx, "service", None)
        if service:
            opts["service"] = {
                "name": getattr(service, "name", None),
                "version": getattr(service, "version", None),
                "node_id": ctx.node_id,
            }

        # Tags
        event_name = getattr(event, "name", str(event))
        opts["tags"] = {
            "event": event_name,
            "caller": ctx.caller,
            "node_id": ctx.node_id,
            "group": getattr(event, "group", None),
        }

        return opts


__all__ = ["TracingMiddleware"]
