__all__ = ["SecretValues"]

from contextlib import contextmanager
from typing import Any, Generator, List


class SecretValues:
    def __init__(self, name: List[str], secret: dict):
        self.name = name
        self.secret = secret
        self._locked = True

    def __getattribute__(self, attr_name: str) -> Any:
        if attr_name != "secret":
            return super().__getattribute__(attr_name)
        else:
            if self._locked:
                raise PermissionError(f"Cannot access locked secret: {self}")
            else:
                return super().__getattribute__(attr_name)

    @contextmanager
    def unlock(self) -> Generator:
        try:
            self._locked = False
            yield self
        finally:
            self._locked = True

    def __repr__(self) -> str:
        if self._locked:
            return f"<{self.__class__.__name__}: {self.name} [LOCKED]>"
        else:
            return f"<{self.__class__.__name__}: {self.name} [UNLOCKED]>"

    def __str__(self) -> str:
        return repr(self)
