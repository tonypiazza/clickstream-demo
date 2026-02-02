# ==============================================================================
# Cache Infrastructure
# ==============================================================================
"""
Cache implementations for the ports-and-adapters architecture.

Available implementations:
- ValkeyCache: Valkey/Redis-based cache with JSON serialization
"""

from clickstream.infrastructure.cache.valkey import (
    ValkeyCache,
    check_valkey_connection,
    get_valkey_cache,
)

__all__ = [
    "ValkeyCache",
    "check_valkey_connection",
    "get_valkey_cache",
]
