# ==============================================================================
# Valkey Cache Implementation
# ==============================================================================
"""
Valkey/Redis implementation of the Cache interface.

Provides:
- Generic key-value storage with TTL
- Batch operations (get_many, set_many)
- Pattern-based deletion
- Atomic counters

Uses JSON serialization for storing dict values.
"""

import json
import logging

import redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from clickstream.base import Cache
from clickstream.utils.config import get_settings
from clickstream.utils.retry import VALKEY_RETRIES

logger = logging.getLogger(__name__)


class ValkeyCache(Cache):
    """
    Valkey/Redis implementation of the Cache interface.

    Configured with:
    - 10 second socket timeouts for fast failure detection
    - Automatic retries with exponential backoff for transient failures
    - Health check interval to keep connections alive

    All values are stored as JSON strings and deserialized on retrieval.
    """

    def __init__(
        self,
        url: str | None = None,
        socket_timeout: int = 10,
        retries: int | None = None,
        health_check_interval: int = 30,
    ):
        """
        Initialize Valkey cache.

        Args:
            url: Valkey/Redis connection URL. If None, uses settings.
            socket_timeout: Socket timeout in seconds (default: 10)
            retries: Number of retries for transient failures (default: from settings)
            health_check_interval: Health check interval in seconds (default: 30)
        """
        if url is None:
            settings = get_settings()
            url = settings.valkey.url

        retry_count = retries if retries is not None else VALKEY_RETRIES
        retry_strategy = Retry(ExponentialBackoff(cap=32, base=1), retries=retry_count)

        self._client = redis.from_url(
            url,
            decode_responses=True,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_timeout,
            retry=retry_strategy,
            retry_on_error=[RedisTimeoutError, RedisConnectionError],
            health_check_interval=health_check_interval,
        )
        self._url = url

    @property
    def client(self) -> redis.Redis:
        """Get the underlying Redis client for advanced operations."""
        return self._client

    def get(self, key: str) -> dict | None:
        """
        Get a cached value.

        Args:
            key: Cache key

        Returns:
            Cached value as dict, or None if not found
        """
        value = self._client.get(key)
        if value is None:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            logger.warning("Failed to decode JSON for key %s", key)
            return None

    def set(self, key: str, value: dict, ttl_seconds: int | None = None) -> None:
        """
        Set a cached value with optional TTL.

        Args:
            key: Cache key
            value: Value to cache (must be JSON-serializable dict)
            ttl_seconds: Optional time-to-live in seconds
        """
        json_value = json.dumps(value)
        if ttl_seconds is not None:
            self._client.setex(key, ttl_seconds, json_value)
        else:
            self._client.set(key, json_value)

    def delete(self, key: str) -> bool:
        """
        Delete a key.

        Args:
            key: Cache key to delete

        Returns:
            True if key was deleted, False if not found
        """
        return self._client.delete(key) > 0

    def get_many(self, keys: list[str]) -> dict[str, dict]:
        """
        Batch get multiple keys.

        Args:
            keys: List of cache keys to fetch

        Returns:
            Dict mapping key to value (missing keys are omitted)
        """
        if not keys:
            return {}

        values = self._client.mget(keys)
        result = {}
        for key, value in zip(keys, values):
            if value is not None:
                try:
                    result[key] = json.loads(value)
                except json.JSONDecodeError:
                    logger.warning("Failed to decode JSON for key %s", key)
        return result

    def set_many(self, items: dict[str, dict], ttl_seconds: int | None = None) -> None:
        """
        Batch set multiple keys with optional TTL.

        Args:
            items: Dict mapping key to value
            ttl_seconds: Optional time-to-live in seconds (applies to all keys)
        """
        if not items:
            return

        pipe = self._client.pipeline()
        for key, value in items.items():
            json_value = json.dumps(value)
            if ttl_seconds is not None:
                pipe.setex(key, ttl_seconds, json_value)
            else:
                pipe.set(key, json_value)
        pipe.execute()

    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a pattern.

        Args:
            pattern: Pattern to match (e.g., "session:*")

        Returns:
            Count of keys deleted
        """
        keys = list(self._client.scan_iter(pattern))
        if keys:
            return self._client.delete(*keys)
        return 0

    def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter. Creates key with value if it doesn't exist.

        Args:
            key: Cache key
            amount: Amount to increment by (default: 1)

        Returns:
            New value after increment
        """
        return self._client.incrby(key, amount)

    def ping(self) -> bool:
        """
        Check if the cache is reachable.

        Returns:
            True if ping succeeds, False otherwise
        """
        try:
            return self._client.ping()
        except Exception:
            return False

    def close(self) -> None:
        """Close the connection."""
        self._client.close()


def get_valkey_cache() -> ValkeyCache:
    """
    Get a ValkeyCache instance configured from settings.

    This is a convenience function that creates a new cache instance
    with default settings. For long-lived applications, consider
    creating a single instance and reusing it.

    Returns:
        Configured ValkeyCache instance
    """
    return ValkeyCache()


def check_valkey_connection() -> bool:
    """
    Check if Valkey is reachable.

    Uses a shorter timeout (5 seconds) since this is just a health check.

    Returns:
        True if Valkey responds to ping, False otherwise
    """
    try:
        settings = get_settings()
        client = redis.from_url(
            settings.valkey.url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        client.ping()
        client.close()
        return True
    except Exception:
        return False
