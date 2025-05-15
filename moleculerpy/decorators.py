from collections.abc import Callable
from typing import Any, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])


def action(
    name: str | None = None,
    params: dict[str, Any] | None = None,
    timeout: float | None = None,
    retries: int | None = None,
    cache_ttl: int | None = None,
    cache: bool | dict[str, Any] | None = None,
) -> Callable[[F], F]:
    """Decorator to mark a method as a service action.

    Args:
        name: Optional custom name for the action. Defaults to function name.
        params: Optional parameter schema for validation.
        timeout: Optional timeout in seconds for remote calls to this action.
                 If not specified, uses broker's default_timeout.
        retries: Optional number of retry attempts for failed calls.
                 Used by RetryMiddleware.
        cache_ttl: Optional cache TTL in seconds for caching results.
                   Legacy option, prefer using cache parameter.
        cache: Cache configuration for this action.
               - True: Enable caching with defaults
               - False: Disable caching
               - dict: Custom config {ttl, keys, lock, enabled}

    Returns:
        Decorator function that marks the method as an action.
    """

    def decorator(func: F) -> F:
        dynamic_func = cast(Any, func)
        dynamic_func._is_action = True
        dynamic_func._name = name if name is not None else func.__name__
        dynamic_func._params = params
        dynamic_func._timeout = timeout
        dynamic_func._retries = retries
        dynamic_func._cache_ttl = cache_ttl
        dynamic_func._cache = cache
        return func

    return decorator


def event(name: str | None = None, params: dict[str, Any] | None = None) -> Callable[[F], F]:
    """Decorator to mark a method as an event handler.

    Args:
        name: Optional custom name for the event. Defaults to function name.
        params: Optional parameter schema for validation.

    Returns:
        Decorator function that marks the method as an event handler.
    """

    def decorator(func: F) -> F:
        dynamic_func = cast(Any, func)
        dynamic_func._is_event = True
        dynamic_func._name = name if name is not None else func.__name__
        dynamic_func._params = params
        return func

    return decorator
