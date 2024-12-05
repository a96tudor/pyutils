from typing import Any, Iterable, Optional

from typing_extensions import SupportsIndex


class Collection(list):
    def __init__(
        self,
        items: Optional[Iterable] = None,
        read_only: bool = False,
    ):
        super().__init__(items or [])
        self.read_only = read_only

    @property
    def read_only(self) -> bool:
        return self._read_only

    @read_only.setter
    def read_only(self, value: bool):
        if not isinstance(value, bool):
            raise TypeError("read_only value must be a boolean")
        self._read_only = value

    def append(self, __object: Any):
        if self.read_only:
            raise ValueError("Cannot append to a read-only collection")
        super().append(__object)

    def extend(self, __iterable: Iterable):
        if self.read_only:
            raise ValueError("Cannot extend a read-only collection")
        super().extend(__iterable)

    def insert(self, __index: SupportsIndex, __object: Any):
        if self.read_only:
            raise ValueError("Cannot insert into a read-only collection")
        super().insert(__index, __object)

    def clone(self, read_only: bool = False) -> "Collection":
        return Collection(self, read_only=read_only)

    def delete(self, __index: SupportsIndex):
        if self.read_only:
            raise ValueError("Cannot delete from a read-only collection")
        del self[__index]

    def pop(self, __index: Optional[SupportsIndex] = None) -> Any:
        if self.read_only:
            raise ValueError("Cannot pop from a read-only collection")
        return super().pop(__index if __index else -1)
