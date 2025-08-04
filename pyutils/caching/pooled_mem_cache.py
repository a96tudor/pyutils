from typing import Any, List, Optional

from pymemcache.client.base import PooledClient

from pyutils.config.providers import ConfigProvider
from pyutils.caching.base import AbstractCacheProvider
from pyutils.logging.logger import Logger

DEFAULT_TIME_TO_LIVE = 300


class StrSerde:
    """
    Serializes and deserializes strings only since
    memcached client defaults to byte strings
    """

    def serialize(self, _, value: str):
        return value.encode("utf-8"), 1

    def deserialize(self, _, value: bytes, flags: int):
        if flags == 1:
            return value.decode("utf-8")
        raise Exception("Unknown serialization format")


class PooledMemcached(AbstractCacheProvider):
    """
    A memcached client to store key/values as a caching mechanism
    """

    GLOBAL_CACHE_STORE: Optional[PooledClient] = None

    def __init__(
        self,
        config_provider: ConfigProvider,
        logger: Logger,
        secrets_config_key: Optional[List[str]] = None,
        use_global_cache: bool = True,
        serde: Optional[Any] = None,
        max_pool_size: int = 2,
        pool_idle_timeout: int = 60,
        timeout: int = 5,
        connect_timeout: int = 5,
        encoding: str = "utf-8",
        decode_responses: bool = True,
        tcp_no_delay: bool = True,
        **kwargs,
    ) -> None:
        """
        Initializes the memcached client using a pooled connection.
        Defaults to a maximum pool size of 2

        The pool idle time is set to 60 seconds default.
          - pooled connections are discarded if they have been
            unused for this many seconds.
            A value of 0 indicates that pooled connections are never discarded.
            To prevent stale connections, we are limiting the minimum idle time to
            5 seconds.

        Specify the secrets config path if it is different than the default
        'elasticache/memcached'

        Pass in a custom serializer/deserializer to handle custom data
        See more on the implementation in the official lib
        https://github.com/pinterest/pymemcache/blob/master/pymemcache/serde.py
        """
        super().__init__()
        self.logger = logger
        self.use_global_cache = use_global_cache
        if not secrets_config_key:
            secrets_config_key = ["elasticache", "memcached"]

        self.config = config_provider.provide(secrets_config_key, secret=False)
        self.cache_url = self.config.get("cache_url", "127.0.0.1")
        self.cache_port = self.config.get("cache_port", "11211")
        self.encoding = encoding or "utf-8"
        self.decode_responses = (
            decode_responses if isinstance(decode_responses, bool) else True
        )
        self.serde = serde or StrSerde()

        if use_global_cache is True:
            max_pool_size = max_pool_size if max_pool_size > 10 else 10
        self.max_pool_size = max_pool_size
        self.connect_timeout = connect_timeout
        self.read_timeout = timeout
        if pool_idle_timeout < 5:
            pool_idle_timeout = 5
        self.pool_idle_timeout = pool_idle_timeout
        self.tcp_no_delay = tcp_no_delay
        self.kwargs = kwargs

        # Keep it local (preferred), since driven by the config value
        self.CACHE_STORE: Optional[PooledClient] = (
            PooledMemcached.GLOBAL_CACHE_STORE
            if self.use_global_cache is True
            else None
        )
        self.init_connection()

    def __del__(self):
        if self.CACHE_STORE is not None and self.use_global_cache is not True:
            # Close all connections
            try:
                self.CACHE_STORE.quit()
            except Exception:
                pass

    def init_connection(self, reinit: bool = False):
        # Create new client pool if it hasn't been created yet
        if self.CACHE_STORE is not None and reinit is False:
            return

        if reinit is True and self.use_global_cache is not True:
            # Close all connections
            try:
                self.CACHE_STORE.quit()
            except Exception:
                pass

        replacement_conn = PooledClient(
            f"{self.cache_url}:{self.cache_port}",
            encoding=self.encoding,
            max_pool_size=self.max_pool_size,
            connect_timeout=self.connect_timeout,
            timeout=self.read_timeout,
            serde=self.serde,
            pool_idle_timeout=self.pool_idle_timeout,
            no_delay=self.tcp_no_delay,
            **self.kwargs,
        )
        if self.use_global_cache is True:
            PooledMemcached.GLOBAL_CACHE_STORE = replacement_conn
            self.CACHE_STORE = PooledMemcached.GLOBAL_CACHE_STORE
        else:
            self.CACHE_STORE = replacement_conn

        # Establish a connection
        self.get_key("__INIT_CONNECTION__")

    def put_key(
        self, cache_key: str, cache_value: str, time_to_live: int = DEFAULT_TIME_TO_LIVE
    ) -> Optional[Any]:
        """
        Sets a key in the cache store with the specified TTL

        :param cache_key: The cache key to store
        :param cache_value: The value to store with the key
        :param time_to_live: The time in seconds before memcache
                             purges the key (defaults to 5 minutes)
        :returns: None
        """
        if not isinstance(time_to_live, int):
            try:
                time_to_live = int(time_to_live)
            except Exception:
                time_to_live = DEFAULT_TIME_TO_LIVE

        try:
            resp = self.CACHE_STORE.set(
                key=cache_key, value=cache_value, expire=time_to_live, noreply=True
            )
            return resp
        except ConnectionResetError:
            self.logger.warning(
                "Memcached - Connection was reset unexpectedly while setting key... "
                "Treating as miss"
            )
        except Exception as ex:
            self.logger.error(
                "Memcached - Exception occurred trying to store key "
                f"Key: '{cache_key}', TTL: {time_to_live}, Error: {repr(ex)}"
            )
        return None

    def get_key(self, cache_key: str) -> Optional[str]:
        """
        Gets the specified key from the cache store

        :param cache_key: The cache key to find
        :returns: The value of the given cache key or None
                  if the key does not exist in the cache
        """
        try:
            resp = self.CACHE_STORE.get(cache_key)
            if resp and self.decode_responses and isinstance(resp, bytes):
                try:
                    return resp.decode(self.encoding)
                except Exception:
                    return resp
            return resp
        except ConnectionResetError:
            self.logger.warning(
                "Memcached - Connection was reset unexpectedly while getting key... "
                "Treating as miss"
            )
        except Exception as ex:
            self.logger.error(
                "Memcached - Exception occurred trying to get key "
                f"Key: '{cache_key}', Error: {repr(ex)}"
            )
        return None

    def delete_key(self, cache_key: str) -> Optional[Any]:
        """
        Deletes the specified key from the cache store

        :param cache_key: The cache key to delete
        :returns: None
        """
        try:
            resp = self.CACHE_STORE.delete(cache_key, noreply=True)
            return resp
        except ConnectionResetError:
            self.logger.warning(
                "Memcached - Connection was reset unexpectedly while deleting key... "
                "Treating as miss"
            )
        except Exception as ex:
            self.logger.error(
                "Memcached - Exception occurred trying to delete key "
                f"Key: '{cache_key}', Error: {repr(ex)}"
            )
        return None

    def increment(self, cache_key: str, count: int) -> Optional[Any]:
        """
        Increments the specified key from the cache store

        :param cache_key: The cache key to Increment
        :returns: None
        """
        try:
            resp = self.CACHE_STORE.incr(cache_key, count, noreply=False)
            return resp
        except ConnectionResetError:
            self.logger.warning(
                "Memcached - Connection was reset unexpectedly while"
                " incrementing key... Treating as miss"
            )
        except Exception as ex:
            self.logger.error(
                "Memcached - Exception occurred trying to increment key "
                f"Key: '{cache_key}', Count: '{count}', Error: {repr(ex)}"
            )
        return None

    def decrement(self, cache_key: str, count: int) -> Optional[Any]:
        """
        Decrements the specified key from the cache store

        :param cache_key: The cache key to decrement
        :returns: None
        """
        try:
            resp = self.CACHE_STORE.decr(cache_key, count, noreply=False)
            return resp
        except ConnectionResetError:
            self.logger.warning(
                "Memcached - Connection was reset unexpectedly while"
                " decrementing key... Treating as miss"
            )
        except Exception as ex:
            self.logger.error(
                "Memcached - Exception occurred trying to decrement key "
                f"Key: '{cache_key}', Count: '{count}',Error: {repr(ex)}"
            )
        return None
