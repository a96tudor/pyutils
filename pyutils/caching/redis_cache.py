from typing import Any, List, Optional

import redis

from pyutils.config.providers import ConfigProvider
from pyutils.logging.logger import Logger
from pyutils.caching.base import AbstractCacheProvider

DEFAULT_TIME_TO_LIVE = 300


class StrSerde:
    """
    Serializes and deserializes strings only since
    redis client defaults to byte strings
    """

    def serialize(self, _, value: str):
        return value.encode("utf-8"), 1

    def deserialize(self, _, value: bytes, flags: int):
        if flags == 1:
            return value.decode("utf-8")
        raise Exception("Unknown serialization format")


class PooledRedis(AbstractCacheProvider):
    """
    A redis client to store key/values as a caching mechanism
    """

    GLOBAL_CACHE_STORE: Optional[redis.Redis] = None

    def __init__(
        self,
        config_provider: ConfigProvider,
        logger: Logger,
        secrets_config_key: Optional[List[str]] = None,
        use_global_cache: bool = True,
        serde: Optional[Any] = None,
        max_pool_size: int = 2,
        timeout=5,
        connect_timeout=5,
        encoding: str = "utf-8",
        decode_responses: bool = True,
        **kwargs,
    ) -> None:
        """
        Initializes the redis client using a pooled connection.
        Defaults to a maximum pool size of 2

        Specify the secrets config path if it is different than the default
        'elasticache/redis'

        """
        super().__init__()
        self.logger = logger
        self.use_global_cache = use_global_cache
        if not secrets_config_key:
            secrets_config_key = ["elasticache", "redis"]

        self.config = config_provider.provide(secrets_config_key, secret=False)
        self.cache_url = self.config.get("cache_url", "127.0.0.1")
        self.cache_port = self.config.get("cache_port", "6379")
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

        # Keep it local (preferred), since driven by the config value
        self.CACHE_STORE: Optional[redis.Redis] = (
            PooledRedis.GLOBAL_CACHE_STORE if self.use_global_cache is True else None
        )
        self.init_connection()

    def __del__(self):
        if self.CACHE_STORE is not None and self.use_global_cache is not True:
            # Close all unused connections
            try:
                self.CACHE_STORE.connection_pool.disconnect(inuse_connections=False)
            except Exception:
                pass

    def init_connection(self, reinit: bool = False):
        # Create new client pool if it hasn't been created yet
        if self.CACHE_STORE is not None and reinit is False:
            return

        if reinit is True and self.use_global_cache is not True:
            # Close all connections
            try:
                self.CACHE_STORE.connection_pool.disconnect(inuse_connections=True)
            except Exception:
                pass

        connection_pool = redis.BlockingConnectionPool(
            # Class specific
            timeout=2,  # This is for Queue timeout
            max_connections=self.max_pool_size,
            # Connection KWARGS
            host=self.cache_url,
            port=self.cache_port,
            encoding=self.encoding,
            decode_responses=self.decode_responses,
            socket_connect_timeout=self.connect_timeout,
            socket_timeout=self.read_timeout,
        )

        replacement_conn = redis.Redis(connection_pool=connection_pool)
        if self.use_global_cache is True:
            PooledRedis.GLOBAL_CACHE_STORE = replacement_conn
            self.CACHE_STORE = PooledRedis.GLOBAL_CACHE_STORE
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
        :param time_to_live: The time in seconds before redis
                             purges the key (defaults to 5 minutes)
        :returns: None
        """
        if not isinstance(time_to_live, int):
            try:
                time_to_live = int(time_to_live)
            except Exception:
                time_to_live = DEFAULT_TIME_TO_LIVE

        try:
            resp = self.CACHE_STORE.set(cache_key, cache_value, ex=time_to_live)
            return resp
        except Exception as ex:
            self.logger.error(
                "Redis - Exception occurred trying to store key "
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
            value = self.CACHE_STORE.get(cache_key)
            if self.decode_responses:
                return value
            elif value:
                return value.decode(self.encoding)
        except Exception as ex:
            self.logger.error(
                "Redis - Exception occurred trying to get key "
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
            resp = self.CACHE_STORE.delete(cache_key)
            return resp
        except Exception as ex:
            self.logger.error(
                "Redis - Exception occurred trying to delete key "
                f"Key: '{cache_key}', Error: {repr(ex)}"
            )
        return None

    def increment(self, cache_key: str, count: int) -> Optional[Any]:
        """
        Increments the specified key from the cache store

        :param cache_key: The cache key to increment
        :returns: None
        """
        try:
            resp = self.CACHE_STORE.incr(cache_key, count)
            return resp
        except Exception as ex:
            self.logger.error(
                "Redis - Exception occurred trying to increment key "
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
            resp = self.CACHE_STORE.decr(cache_key, count)
            return resp
        except Exception as ex:
            self.logger.error(
                "Redis - Exception occurred trying to decrement key "
                f"Key: '{cache_key}', Count: '{count}', Error: {repr(ex)}"
            )
        return None
