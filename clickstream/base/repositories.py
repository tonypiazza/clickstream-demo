# ==============================================================================
# Repository Abstract Base Classes
# ==============================================================================
"""
Repository ABCs for data persistence.

These define the "what" (save data) not the "how" (insert vs upsert).
Concrete implementations in infrastructure/ handle the specifics.

Includes:
- EventRepository: Clickstream event persistence
- SessionRepository: Session aggregation persistence
- SearchRepository: Search/analytics indexing

Note: Cache and SessionStateStore are in separate modules (cache.py, session_state.py)
since they are not traditional repositories (collections of domain objects).
"""

from abc import ABC, abstractmethod


class EventRepository(ABC):
    """Repository for clickstream events."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data store."""
        ...

    @abstractmethod
    def save(self, events: list[dict]) -> int:
        """
        Persist events.

        Args:
            events: List of event dictionaries to persist

        Returns:
            Count of events saved
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Close connection and release resources."""
        ...


class SessionRepository(ABC):
    """Repository for session aggregations."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data store."""
        ...

    @abstractmethod
    def save(self, sessions: list[dict]) -> int:
        """
        Persist sessions.

        Args:
            sessions: List of session dictionaries to persist

        Returns:
            Count of sessions saved
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Close connection and release resources."""
        ...


class SearchRepository(ABC):
    """Repository for search/analytics indexing."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to search engine."""
        ...

    @abstractmethod
    def save(self, documents: list[dict]) -> int:
        """
        Index documents.

        Args:
            documents: List of document dictionaries to index

        Returns:
            Count of documents indexed
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Close connection and release resources."""
        ...
