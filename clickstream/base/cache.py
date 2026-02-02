# ==============================================================================
# Cache Abstract Base Class
# ==============================================================================
"""
Abstract interface for key-value caching with TTL support.

This is NOT a repository (which represents domain object collections).
Cache is transient storage for performance optimization.

Implementations: Valkey, Redis, Memcached, in-memory, etc.
"""

from abc import ABC, abstractmethod


class Cache(ABC):
    """
    Generic cache interface for key-value storage with TTL support.

    All values are stored as dicts (JSON-serializable). Implementations
    handle serialization/deserialization internally.
    """

    @abstractmethod
    def get(self, key: str) -> dict | None:
        """
        Get a cached value.

        Args:
            key: Cache key

        Returns:
            Cached value as dict, or None if not found
        """
        ...

    @abstractmethod
    def set(self, key: str, value: dict, ttl_seconds: int | None = None) -> None:
        """
        Set a cached value with optional TTL.

        Args:
            key: Cache key
            value: Value to cache (must be JSON-serializable dict)
            ttl_seconds: Optional time-to-live in seconds
        """
        ...

    @abstractmethod
    def delete(self, key: str) -> bool:
        """
        Delete a key.

        Args:
            key: Cache key to delete

        Returns:
            True if key was deleted, False if not found
        """
        ...

    @abstractmethod
    def get_many(self, keys: list[str]) -> dict[str, dict]:
        """
        Batch get multiple keys.

        Args:
            keys: List of cache keys to fetch

        Returns:
            Dict mapping key to value (missing keys are omitted)
        """
        ...

    @abstractmethod
    def set_many(self, items: dict[str, dict], ttl_seconds: int | None = None) -> None:
        """
        Batch set multiple keys with optional TTL.

        Args:
            items: Dict mapping key to value
            ttl_seconds: Optional time-to-live in seconds (applies to all keys)
        """
        ...

    @abstractmethod
    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a pattern.

        Args:
            pattern: Pattern to match (e.g., "session:*")

        Returns:
            Count of keys deleted
        """
        ...

    @abstractmethod
    def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter. Creates key with value if it doesn't exist.

        Args:
            key: Cache key
            amount: Amount to increment by (default: 1)

        Returns:
            New value after increment
        """
        ...
