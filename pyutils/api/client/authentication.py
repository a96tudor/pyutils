import abc
from datetime import datetime, timedelta
from typing import List, Optional

import requests  # type: ignore[import-untyped]

from pyutils.api.client.errors import AuthenticationError
from pyutils.config.providers import ConfigProvider
from pyutils.config.secrets import SecretValues
from pyutils.logging.logger import Logger


class Authenticator(abc.ABC):
    def __init__(
        self,
        api_name: str,
        config: Optional[ConfigProvider],
        logger: Optional[Logger] = None,
    ) -> None:
        self.api_name = api_name
        self._config = config
        self._token: Optional[str] = None
        self._expiration_date: Optional[datetime] = None
        self._logger: Optional[Logger] = logger

    def _is_valid(self) -> bool:
        return (
            self._expiration_date is not None and self._expiration_date > datetime.now()
        )

    @abc.abstractmethod
    def _generate_token(self) -> str:
        pass

    @property
    def token(self) -> str:
        if not self._is_valid():
            self._token = self._generate_token()
        return self._token or ""

    @classmethod
    @abc.abstractmethod
    def from_config_provider(
        cls,
        api_name: str,
        config_provider: ConfigProvider,
        config_key: List[str],
        logger: Optional[Logger] = None,
    ) -> "Authenticator":
        pass

    @abc.abstractmethod
    @property
    def auth_header(self) -> dict:
        pass


class OAuthAuthenticator(Authenticator):
    _TOKEN_REQUEST_HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

    def __init__(
        self,
        api_name: str,
        config: ConfigProvider,
        config_keys: List[str],
        *,
        grant_type: Optional[str] = None,
        logger: Optional[Logger] = None,
    ) -> None:
        super().__init__(api_name, config, logger)
        self._grant_type = grant_type if grant_type else "client_credentials"
        assert self._config is not None
        self._config_values: SecretValues
        self._config_values = self._config.provide(  # type: ignore[assignment]
            config_keys, secret=True
        )

    def __build_get_token_request_body(self) -> dict:
        token_req_body = {
            "client_id": self._config_values.secret["client_id"],
            "client_secret": self._config_values.secret["client_secret"],
            "grant_type": self._config_values,
        }

        if "scope" in self._config_values.secret:
            token_req_body["scope"] = self._config_values.secret["scope"]

        return token_req_body

    def __process_raw_token_response(self, raw_response: requests.Response) -> None:
        if raw_response is None:
            raise AuthenticationError(self.api_name)

        if raw_response.status_code != 200:
            if self._logger:
                self._logger.error(
                    f"Failed to authenticate with OAuth to {self.api_name}. "
                    f"Expected code 200, got {raw_response.status_code}. "
                    f"Full response: {raw_response}."
                )

        decoded_response = raw_response.json()
        ttl = decoded_response["expires_in"]
        self._expiration_date = datetime.now() + timedelta(seconds=ttl)
        self._token = decoded_response["access_token"]

    def _generate_token(self) -> str:
        raw_response = None
        try:
            with self._config_values.unlock():
                req_body = self.__build_get_token_request_body()
                url = self._config_values.secret["access_token_url"]

                raw_response = requests.post(
                    url, data=req_body, headers=self._TOKEN_REQUEST_HEADERS
                )
        except Exception as e:
            if self._logger:
                self._logger.error(e)
            raise AuthenticationError(self.api_name)

        self.__process_raw_token_response(raw_response)
        return self._token or ""

    @classmethod
    def from_config_provider(
        cls,
        api_name: str,
        config_provider: ConfigProvider,
        config_key: List[str],
        logger: Optional[Logger] = None,
    ) -> "OAuthAuthenticator":
        return cls(
            api_name,
            config_provider,
            config_keys=config_key,
            logger=logger,
        )

    @property
    def auth_header(self) -> dict:
        return {"Authorization": f"Bearer {self._token}"}


class APIKeyAuthenticator(Authenticator):
    def __init__(
        self,
        api_name: str,
        config: ConfigProvider,
        config_key: List[str],
        *,
        logger: Optional[Logger] = None,
    ):
        super().__init__(api_name, config, logger)
        assert self._config is not None
        self._config_values: SecretValues
        self._config_values = self._config.provide(  # type: ignore[assignment]
            config_key, secret=True
        )

    def _generate_token(self) -> str:
        with self._config_values.unlock():
            self._token = self._config_values.secret["api_key"]
            expires_in = self._config_values.secret.get("expires_in", 500)

        self._expiration_date = datetime.now() + timedelta(seconds=expires_in)

        return self._token or ""

    @classmethod
    def from_config_provider(
        cls,
        api_name: str,
        config_provider: ConfigProvider,
        config_key: List[str],
        logger: Optional[Logger] = None,
    ) -> "APIKeyAuthenticator":
        return cls(
            api_name,
            config_provider,
            config_key=config_key,
            logger=logger,
        )

    @property
    def auth_header(self) -> dict:
        return {"API_KEY": self._token}
