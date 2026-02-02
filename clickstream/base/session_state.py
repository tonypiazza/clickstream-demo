# ==============================================================================
# Session State Store Abstract Base Class
# ==============================================================================
"""
Abstract interface for session state persistence in streaming pipelines.

This is a higher-level interface for session-specific operations.
Implementations typically wrap a Cache for storage.

Used by streaming consumers to persist session state between batches,
enabling session aggregation across events.
"""

from abc import ABC, abstractmethod


class SessionStateStore(ABC):
    """
    Store for session state persistence in streaming pipelines.

    Higher-level interface for session-specific operations.
    Implementations typically wrap a Cache for storage.
    """

    @abstractmethod
    def get_sessions(self, visitor_ids: list[int]) -> dict[int, dict]:
        """
        Get session states for multiple visitors.

        Args:
            visitor_ids: List of visitor IDs to fetch

        Returns:
            Dict mapping visitor_id to session dict.
            Missing visitors are omitted from the result.
        """
        ...

    @abstractmethod
    def save_sessions(self, sessions: dict[int, dict], ttl_seconds: int) -> None:
        """
        Save session states with TTL.

        Args:
            sessions: Dict mapping visitor_id to session dict
            ttl_seconds: Time-to-live for the cached sessions
        """
        ...

    @abstractmethod
    def clear_all(self) -> int:
        """
        Clear all session state.

        Returns:
            Count of sessions deleted
        """
        ...
