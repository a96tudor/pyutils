"""
Provides an interface for implementing a cache store for any purpose

See beppy/helpers/cache for example implementations
"""

from abc import ABC, abstractmethod
from typing import Any, Optional


class AbstractCacheProvider(ABC):
    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        # Creates a class level CACHE variable that can be shared amongst
        #  class instances whenever this abstract class gets subclassed
        cls.CACHE: Optional[Any] = None

    @abstractmethod
    def put_key(
        self, cache_key: str, cache_value: Any, time_to_live: int = 0
    ) -> Optional[bool]:
        """
        Given a cache key and value, store the key/value pair in the cache
        If the cache implementation supports a time_to_live value, then use it
        """
        ...

    @abstractmethod
    def get_key(self, cache_key: str) -> Optional[Any]:
        """
        Retrieves a value from the cache store using the given key
        """
        ...

    @abstractmethod
    def delete_key(self, cache_key: str) -> Optional[bool]:
        """
        Deletes the given key from the cache store
        """
        ...
