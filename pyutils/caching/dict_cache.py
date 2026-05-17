from typing import Any, ClassVar, Optional

from pyutils.caching.base import AbstractCacheProvider


class DictCache(AbstractCacheProvider):
    """Provide a basic cache in the form of a dictionary."""

    CACHE: ClassVar[Optional[dict]] = None

    def __init__(self) -> None:
        super().__init__()

        # Create new client pool if it hasn't been created yet
        if DictCache.CACHE is None:
            DictCache.CACHE = {}

    def get_key(self, cache_key: str) -> Optional[Any]:
        resp = DictCache.CACHE.get(cache_key)  # type: ignore[union-attr]
        return resp

    def put_key(
        self, cache_key: str, cache_value: Any, time_to_live: int = 0
    ) -> Optional[Any]:
        DictCache.CACHE[cache_key] = cache_value  # type: ignore[index]
        return True

    def delete_key(self, cache_key: str) -> Optional[Any]:
        resp = DictCache.CACHE.pop(cache_key, None)  # type: ignore[union-attr]
        return resp
