from typing import Any, Optional

from pyutils.caching.base import AbstractCacheProvider


class DictCache(AbstractCacheProvider):
    """
    Provides a basic cache in the form of a dictionary
    """

    def __init__(self) -> None:
        super().__init__()

        # Create new client pool if it hasn't been created yet
        if DictCache.CACHE is None:
            DictCache.CACHE: dict = {}

    def get_key(self, cache_key: str) -> Optional[Any]:
        resp = DictCache.CACHE.get(cache_key)
        return resp

    def put_key(
        self, cache_key: str, cache_value: Any, time_to_live: int = 0
    ) -> Optional[Any]:
        DictCache.CACHE[cache_key] = cache_value
        return True

    def delete_key(self, cache_key: str) -> Optional[Any]:
        resp = DictCache.CACHE.pop(cache_key, None)
        return resp
