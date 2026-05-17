import abc
from typing import List, Optional

from pyutils.api.client.authentication import Authenticator
from pyutils.config.providers import ConfigProvider
from pyutils.logging import Logger


class BaseAPIClient(abc.ABC):
    def __init__(
        self,
        config_provider: ConfigProvider,
        config_key: List[str],
        api_name: str,
        *,
        authenticator_type: Optional[Authenticator] = None,
        logger: Optional[Logger] = None,
    ):
        self.api_name = api_name
        self.config_provider = config_provider
        self.config_key = config_key
        if authenticator_type:
            self._authenticator = authenticator_type.from_config_provider(
                api_name=api_name,
                config_provider=config_provider,
                config_key=config_key,
                logger=logger,
            )
        else:
            self._authenticator = None
        self._base_url = None
        self._logger = logger
