__all__ = ["set_execution_id", "get_execution_id", "clear_execution_id"]

from contextvars import ContextVar
from typing import Optional

_EXECUTION_ID: ContextVar[Optional[str]] = ContextVar("execution_id", default=None)


def set_execution_id(execution_id: str) -> None:
    """Set the execution ID for the current context."""
    _EXECUTION_ID.set(execution_id)


def get_execution_id() -> Optional[str]:
    """Return the execution ID for the current context if set."""
    return _EXECUTION_ID.get()


def clear_execution_id() -> None:
    """Clear the execution ID for the current context."""
    _EXECUTION_ID.set(None)
