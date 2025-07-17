import os
import abc
from typing import Optional

from pyutils.helpers.secrets import SecretsManager


class ConfigProvider(abc.ABC):
    """
    A base class for configuration providers.
    This class should be extended by specific configuration providers.
    """
    def __init__(self):
        """
        Initialize the configuration provider.
        This method can be overridden by subclasses to perform custom initialization.
        """
        self.__secrets_manager: Optional[SecretsManager] = None

    @abc.abstractmethod
    def __get_attribute__(self, key: str) -> Optional[str]:
        """
        Retrieve a configuration value by its key.
        Returns None if the key does not exist.
        """
        raise NotImplementedError("Subclasses must implement this method.")


class OSConfigProvider(ConfigProvider):
    """
    A SecretsManager that loads secrets from environment variables.
    This is a simple implementation for demonstration purposes.
    """

    def __get_attribute__(self, key: str) -> Optional[str]:
        """
        Retrieve a secret from environment variables.
        Returns None if the secret does not exist.
        """
        return os.getenv(key)
