from pyutils.helpers.errors import Error


class RetryConnectionError(Error):
    """Exception raised when a connection retry is failing."""

    _extension_details = {
        "category": "server",
        "code": "RetryConnectionError",
        "severity": "warning",
    }

    def __init__(self, message="Retrying connection failed.", *args, **kwargs):
        super().__init__(message, *args, **kwargs)
