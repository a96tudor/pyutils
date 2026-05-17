from pyutils.helpers.errors import Error


class APIClientError(Error):
    _extension_details = {
        "category": "server",
        "code": "API Client Error",
        "severity": "error",
    }

    def __init__(self, api_name: str, message: str) -> None:
        super().__init__(f"Failed to connect to {api_name} server: {message}.")

    def __str__(self) -> str:
        return self.message


class AuthenticationError(Error):

    _extension_details = {
        "category": "authentication",
        "code": "Authentication " "Error",
        "severity": "error",
    }

    def __init__(self, api_name: str) -> None:
        super().__init__(f"Authentication to {api_name} failed.")
