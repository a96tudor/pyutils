import json
import re
from contextvars import ContextVar
from logging import INFO, Formatter, Handler
from logging import Logger as Logger_
from typing import Iterable, List, Optional, Union

from pyutils.decorators.singleton import singleton

_execution_id: ContextVar[Optional[str]] = ContextVar("execution_id", default=None)


@singleton
class Logger(Logger_):
    __JSON_IN_STRING_REGEX = re.compile(r"({.*?}|\[.*?\])")

    def __init__(
        self,
        name: str,
        formatter: Formatter,
        handler: Handler,
        level: Optional[Union[int, str]] = INFO,
        pii_keys: Optional[Iterable[str]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(name, *args, **kwargs)
        self.setLevel(level)
        handler.setFormatter(formatter)
        self.addHandler(handler)
        self.propagate = False
        self.formatter = formatter
        self.__pii_keys = pii_keys if pii_keys else []

    def set_execution_id(self, execution_id: str) -> None:
        """Set the execution ID for the current context."""
        _execution_id.set(execution_id)

    def get_execution_id(self) -> Optional[str]:
        """Return the execution ID for the current context if set."""
        return _execution_id.get()

    def clear_execution_id(self) -> None:
        """Clear the execution ID for the current context."""
        _execution_id.set(None)

    def __find_json_patterns_in_message(self, msg: str) -> List[str]:
        """
        Find and return JSON strings in the message.
        """
        return self.__JSON_IN_STRING_REGEX.findall(msg)

    def __scrub_pii(self, obj):
        """Recursively scrub PII from a dict or list."""
        if isinstance(obj, dict):
            return {
                k: (
                    "***REDACTED***"
                    if k.lower() in self.__pii_keys
                    else self.__scrub_pii(v)
                )
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [self.__scrub_pii(item) for item in obj]
        return obj

    def __scrub_json_in_text(self, text: str) -> str:
        """Find JSON in text, scrub it, and return cleaned text."""
        if not self.__pii_keys:
            return text

        cleaned_text = text
        json_candidates = self.__find_json_patterns_in_message(text)

        for candidate in json_candidates:
            try:
                parsed = json.loads(candidate)
                cleaned = self.__scrub_pii(parsed)
                cleaned_json = json.dumps(cleaned)
                cleaned_text = cleaned_text.replace(candidate, cleaned_json)
            except json.JSONDecodeError:
                continue  # Not actually valid JSON

        return cleaned_text

    def _log(self, level: int, msg: str, *args, **kwargs) -> None:
        """
        Override the _log method to ensure the message is formatted correctly.
        """
        msg = self.__scrub_json_in_text(msg)
        extra = kwargs.pop("extra", {}) or {}
        execution_id = self.get_execution_id()
        if execution_id is not None and "execution_id" not in extra:
            extra["execution_id"] = execution_id
        kwargs["extra"] = extra

        super()._log(level, msg, *args, **kwargs)
