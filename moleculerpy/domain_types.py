"""Domain primitive types for stronger static typing.

These aliases improve readability and catch mixups (e.g. passing ActionName where
NodeID is expected) without changing runtime behavior.
"""

from typing import NewType

NodeID = NewType("NodeID", str)
ServiceName = NewType("ServiceName", str)
ActionName = NewType("ActionName", str)
EventName = NewType("EventName", str)
ContextID = NewType("ContextID", str)
RequestID = NewType("RequestID", str)

__all__ = [
    "ActionName",
    "ContextID",
    "EventName",
    "NodeID",
    "RequestID",
    "ServiceName",
]
