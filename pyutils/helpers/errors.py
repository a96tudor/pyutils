class Error(Exception):
    """Base class for all custom exceptions in the application."""

    _extension_details = {"category": "server", "code": "Error", "severity": "error"}

    def __init__(self, message=None, *args, **kwargs):
        try:
            # Maybe the caller forgot to pass the error msg as a message kwarg.
            # Nevertheless, it was passed in as an arg value
            if not message and args:
                message = f"{args}"
        except Exception:
            pass

        if not message:
            message = "Internal Server Error."
        self.message = message

        self._setattrs = ["message"]
        # allow details to be overridden
        for attr, val in self._extension_details.items():
            setattr(self, attr, val)
            self._setattrs.append(attr)
        self.__dict__.update(kwargs)
        Exception.__init__(self, message)

    def __repr__(self) -> str:
        formatted_attrs = ", ".join(
            [f"{attr}='{getattr(self, attr, None)}'" for attr in self._setattrs]
        )
        return f"{self.__class__.__name__}({formatted_attrs})"

    @property
    def extension_details(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


class LockedSecretError(Error):
    """Exception raised when trying to access a locked secret."""

    _extension_details = {
        "category": "server",
        "code": "LockedSecretError",
        "severity": "error",
    }

    def __init__(self, message="Attempted to access a locked secret.", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class BadArgumentsError(Error):
    """Exception raised for invalid arguments."""

    _extension_details = {
        "category": "server",
        "code": "BadArgumentsError",
        "severity": "error",
    }

    def __init__(self, message="Invalid argument provided.", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class MethodNotAvailableError(Error):
    _extension_details = {
        "category": "server",
        "code": "MethodNotAvailableError",
        "severity": "error",
    }

    def __init__(
        self,
        message="This method is not available in the current context.",
        *args,
        **kwargs,
    ):
        super().__init__(message, *args, **kwargs)


class DataValidationError(Error):
    """Exception raised when there is an issue with data validation."""

    _extension_details = {
        "category": "server",
        "code": "DataValidationError",
        "severity": "error",
    }
