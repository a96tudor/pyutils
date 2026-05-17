import abc
import json
from typing import Callable, List, Optional, Union
from urllib.parse import urlencode, urljoin

import requests  # type: ignore[import-untyped]

from pyutils.api.client.authentication import Authenticator
from pyutils.api.client.base_client import BaseAPIClient
from pyutils.api.client.rest.errors import RESTClientError
from pyutils.config.providers import ConfigProvider
from pyutils.logging.logger import Logger


class RESTClient(BaseAPIClient, abc.ABC):
    def __init__(
        self,
        config_provider: ConfigProvider,
        config_key: List[str],
        api_name: str,
        *,
        authenticator_type: Optional[Authenticator] = None,
        user_agent: Optional[str] = None,
        logger: Optional[Logger] = None,
    ):
        super().__init__(
            config_provider,
            config_key,
            api_name,
            authenticator_type=authenticator_type,
            logger=logger,
        )
        self.user_agent = user_agent

    @property
    def base_url(self) -> str:
        if self._base_url is None:
            config_values = self.config_provider.provide(self.config_key)
            with config_values.unlock():  # type: ignore[union-attr]
                scheme = config_values["scheme"]  # type: ignore[index]
                host = config_values["host"]  # type: ignore[index]
                base_path = config_values["base_path"]  # type: ignore[index]

            self._base_url = urljoin(f"{scheme}://{host}", base_path)

        return self._base_url

    def _get_headers(self, data: Optional[Union[dict, list]] = None) -> dict:
        headers = {}

        if self._authenticator:
            headers.update(self._authenticator.auth_header)

        if self.user_agent:
            headers["User-Agent"] = self.user_agent

        if data:
            headers["Content-Type"] = "application/json"

        return headers

    def _build_url(self, path: str, url_filters: Optional[dict] = None) -> str:
        full_path = urljoin(self.base_url, path)
        if url_filters:
            filters = urlencode(url_filters)
            full_path = f"{full_path}?{filters}"

        return full_path

    @abc.abstractmethod
    def _validate(self, response: dict, path: str) -> bool:
        pass

    def _make_request(
        self,
        path: str,
        func: Callable,
        data: Optional[Union[dict, list]] = None,
        url_filters: Optional[dict] = None,
    ) -> dict:
        raw_result = None
        try:
            raw_result = None
            full_path = self._build_url(path, url_filters)
            headers = self._get_headers(data)
            raw_result = func(full_path, data=json.dumps(data), headers=headers)

            response = raw_result.json()
        except Exception as err:
            if raw_result is None:
                raise RESTClientError(
                    self.api_name,
                    "Failed to make request",
                    self.base_url,
                    path,
                    "500",
                )
            if raw_result.status_code == 204:
                return {}
            if raw_result is not None and hasattr(raw_result, "text"):
                raw_result = raw_result.text
            raise RESTClientError(
                self.api_name, f"{str(err)}: {raw_result}", self.base_url, path, "500"
            )

        if response:
            self._validate(response, path)

        return response

    def get(self, path: str, url_filters: Optional[dict] = None) -> Union[dict, list]:
        return self._make_request(path, func=requests.get, url_filters=url_filters)

    def post(self, path: str, data: Union[dict, list]) -> dict:
        return self._make_request(path, func=requests.post, data=data)

    def put(self, path: str, data: Union[dict, list]) -> dict:
        return self._make_request(path, func=requests.put, data=data)

    def patch(self, path: str, data: dict) -> dict:
        return self._make_request(path, func=requests.patch, data=data)

    def delete(self, path: str) -> dict:
        return self._make_request(path, func=requests.delete)
