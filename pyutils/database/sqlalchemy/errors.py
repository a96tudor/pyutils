from typing import Optional

from pyutils.database.errors import DatabaseError


class SqlAlchemyError(DatabaseError):
    """Base class for a SQLAlchemy-related error."""

    _extension_details = {
        "category": "server",
        "code": "SqlAlchemyError",
        "severity": "error",
    }

    def __init__(self, message: str, *args, **kwargs):
        super().__init__(message, engine="SQLAlchemy", *args, **kwargs)


class SqlAlchemyConnectionError(SqlAlchemyError):
    """Exception raised when a connection to the database fails."""

    _extension_details = {
        "category": "server",
        "code": "SqlAlchemyConnectionError",
        "severity": "error",
    }

    def __init__(
        self,
        message: Optional[str] = "Failed to connect to the database.",
        error: Optional[Exception] = None,
        *args,
        **kwargs,
    ):
        super().__init__(f"{message}: {error}" if error else message, *args, **kwargs)


class SQLAlchemySessionError(SqlAlchemyError):
    """Exception raised when there is an issue with the SQLAlchemy session."""

    _extension_details = {
        "category": "server",
        "code": "SQLAlchemySessionError",
        "severity": "error",
    }

    def __init__(
        self,
        message: Optional[str] = "An error occurred with the SQLAlchemy session.",
        error: Optional[Exception] = None,
        *args,
        **kwargs,
    ):
        super().__init__(f"{message}: {error}" if error else message, *args, **kwargs)
