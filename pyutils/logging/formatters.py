import abc
import json
from datetime import datetime, timezone
from typing import Any, Optional, Union
from uuid import UUID

from pyutils.helpers.uuid import validate_uuid
from pyutils.logging.enum import LogLevel

DEFAULT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class LogFormatter(abc.ABC):
    """
    Abstract base class for log formatters.
    Subclasses should implement the `format` method.
    """

    def __init__(
        self,
        environment: str,
        api_version: str,
        time_format: Optional[str] = None,
    ):
        if time_format is None:
            self.__time_format = DEFAULT_TIME_FORMAT
        else:
            self.__time_format = time_format

        self.__environment = environment
        self.__api_version = api_version

    @property
    def current_time(self) -> str:
        return datetime.now(timezone.utc).strftime(self.__time_format)

    @abc.abstractmethod
    def format(
        self,
        level: LogLevel,
        message: str,
        execution_id: Union[str, UUID],
        username: str,
    ) -> Any:
        raise NotImplementedError()


class JSONLogFormatter(LogFormatter):
    """
    A log formatter that formats log records as JSON strings.
    """

    def format(
        self,
        level: LogLevel,
        message: str,
        execution_id: Union[str, UUID],
        username: str,
    ) -> str:
        return json.dumps(
            {
                "execution_id": str(execution_id),
                "timestamp": self.current_time,
                "level": level.value,
                "message": message,
                "username": username,
                "environment": self.__environment,
                "api_version": self.__api_version,
            }
        )
