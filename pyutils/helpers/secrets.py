__all__ = ["SecretsManager"]

import abc
from typing import Optional, Any
import json

from contextlib import contextmanager
import hvac

from pyutils.helpers.errors import LockedSecretError, MethodNotAvailableError

class SecretValues:
    def __init__(self, name: str, secret: dict):
        self.__name = name
        self.__secret = secret
        self.__locked = True

    def __getattribute__(self: "SecretValues", attr_name: str) -> Any:
        if attr_name in ("__name", "__secret", "__locked"):
            raise PermissionError(f"Access denied")


class SecretsManager:
    def __init__(self):
        self.__secrets: dict[str, str] = {}
        self.__locked: bool = True

    def __get_attribute__(self) -> Optional[str]:
        """
        Retrieve a secret by its key.
        Returns None if the secret is locked or does not exist.
        """
        if
        if self.__locked:
            raise LockedSecretError()
        return self.__secrets
    def add_secret(self, key: str, value: str) -> None:
        """
        Add a new secret to the manager.
        """
        new_secret = Secret(key, value)
        self.__secrets[key] = new_secret

    def get_secret(self, key: str) -> Optional[Secret]:
        """
        Retrieve a secret by its key.
        Returns None if the secret is locked or does not exist.
        """
        if self.__locked:
            raise LockedSecretError()
        secret = self.__secrets.get(key)
        if secret is None:
            return None
        with secret.unlock():
            return secret.value

    @contextmanager
    def unlock(self) -> "Secret":
        """
        Temporarily unlock credentials.
        Automatically locks them again after use.
        """
        try:
            self.__locked = False
            yield self
        finally:
            self.__locked = True
            # Optional: wipe credentials or log access


class JSONSecretsManager(SecretsManager):
    def __init__(self, file_path: str):
        super().__init__()

        with open(file_path, "r") as f:
            secrets = json.load(f)
            for key, value in secrets.items():
                self.add_secret(key, value)


class HVACSecretsManager(SecretsManager, abc.ABC):
    def __init__(
        self,
        url_env_var_name: str,
        token_env_var_name: str,
        role_id_env_var_name: str,
        secret_id_env_var_name: str,
    ):
        super().__init__()

        self.__url_env_var_name = url_env_var_name
        self.__token_env_var_name = token_env_var_name
        self.__role_id_env_var_name = role_id_env_var_name
        self.__secret_id_env_var_name = secret_id_env_var_name

        self.__client_instance = None

    @abc.abstractmethod
    @property
    def __local_config(self) -> SecretsManager:
        raise NotImplementedError()

    def add_secret(self, key: str, value: str) -> None:
        raise MethodNotAvailableError

    @property
    def __client(self):
        if self.__client_instance and self.__client_instance.is_authenticated():
            return self.__client_instance

        with self.__local_config.unlock() as s:
            if self.__client_instance is None:
                self.__client_instance = hvac.Client(
                    url=s.get_secret(self.__url_env_var_name),
                    token=s.get_secret(self.__token_env_var_name),
                )

            return self.__client_instance.auth.approle.login(
                role_id = s.get_secret(self.__role_id_env_var_name),
                secret_id = s.get_secret(self.__url_env_var_name),
            )

    def get_secret(self, key: str) -> Optional[Secret]:
        if self.__locked:
            raise LockedSecretError()


