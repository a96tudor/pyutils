class Error(Exception):
    """Base class for all custom exceptions in the application."""
    pass


class LockedSecretError(Error):
    """Exception raised when trying to access a locked secret."""

    def __init__(self, message="Attempted to access a locked secret."):
        super().__init__(message)


class BadArgumentsError(Error):
    """Exception raised for invalid arguments."""

    def __init__(self, message="Invalid argument provided."):
        super().__init__(message)


class MethodNotAvailableError(Error):
    def __init__(self):
        super().__init__("This method is not available in the current context.")
