from pyutils.api.client.errors import APIClientError


class RESTClientError(APIClientError):
    _extension_details = {
        "category": "server",
        "code": "REST API Client Error",
        "severity": "error",
    }

    def __init__(
        self, api_name: str, message: str, base_url: str, path: str, error_code: str
    ):
        super().__init__(
            api_name,
            f"REST Client error with code {error_code} when requesting at "
            f"URL {base_url}, path {path}: {message}.",
        )

        self.message = (
            f"REST Client error with code {error_code} when requesting at "
            f"URL {base_url}, path {path}: {message}."
        )

        self.error_code = error_code
        self.path = path
        self.base_url = base_url
        self._raw_message = message
