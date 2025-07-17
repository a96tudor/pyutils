import abc

from pyutils.logging.enum import LogLevel
from pyutils.logging.formatters import LogFormatter


class LoggerBase(abc.ABC):
    def __init__(self, formatter: LogFormatter):
        self.formatter = formatter

    @abc.abstractmethod
    def _write_log(self, log: str):
        raise NotImplementedError()

    def log(
        self, level: LogLevel, message: str, execution_id: str, username: str
    ) -> None:
        formatted_message = self.formatter.format(
            level, message, execution_id, username
        )
        self._write_log(formatted_message)

    def info(self, message: str, execution_id: str, username: str) -> None:
        self.log(LogLevel.INFO, message, execution_id, username)

    def debug(self, message: str, execution_id: str, username: str) -> None:
        self.log(LogLevel.DEBUG, message, execution_id, username)

    def warning(self, message: str, execution_id: str, username: str) -> None:
        self.log(LogLevel.WARNING, message, execution_id, username)

    def error(self, message: str, execution_id: str, username: str) -> None:
        self.log(LogLevel.ERROR, message, execution_id, username)
