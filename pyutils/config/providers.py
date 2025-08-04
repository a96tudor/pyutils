import abc
import json
from pathlib import Path
from typing import Callable, List, Optional, TextIO, Union

import yaml

from pyutils.config.secrets import SecretValues
from pyutils.datatools.general import get_in


class ConfigProvider(abc.ABC):

    def __init__(self, base_config_path: Optional[List[str]] = None):
        self.__value: Optional[SecretValues] = None
        self.__base_config_path = base_config_path

    @abc.abstractmethod
    def provide(self, config_path: List[str]) -> Optional[SecretValues]:
        raise NotImplementedError("Subclasses must implement this method.")


class FileConfigProvider(ConfigProvider):

    def __init__(
        self,
        file_loader: Callable[[TextIO], dict],
        config_filename: Union[str, Path],
        base_config_path: Optional[List[str]] = None,
    ) -> None:
        super().__init__(base_config_path=base_config_path)
        self.config_filename = config_filename
        self.file_loader = file_loader
        self.loaded_config: Optional[dict] = None

    def provide(
        self, config_path: List[str], secret: Optional[bool] = True
    ) -> Union[SecretValues, dict]:
        if self.loaded_config is None:
            with open(self.config_filename) as config_file:
                self.loaded_config = self.file_loader(config_file)

        # Declared here because pyre type checks doesn't detect the 'if' test below
        # Access attribute from ConfigProvider; avoid name mangling issues
        base_path = self._ConfigProvider__base_config_path
        if base_path:
            config_value = get_in(self.loaded_config, base_path + config_path)
        else:
            config_value = get_in(self.loaded_config, config_path)

        if secret:
            return SecretValues(config_path, config_value)  # type: ignore
        else:
            return config_value


class YAMLConfigProvider(FileConfigProvider):

    def __init__(
        self,
        config_filename: Union[str, Path],
        base_config_path: Optional[List[str]] = None,
    ) -> None:
        super().__init__(
            yaml.safe_load, config_filename, base_config_path=base_config_path
        )


class JSONConfigProvider(FileConfigProvider):

    def __init__(
        self,
        config_filename: Union[str, Path],
        base_config_path: Optional[List[str]] = None,
    ) -> None:
        super().__init__(json.load, config_filename, base_config_path=base_config_path)
