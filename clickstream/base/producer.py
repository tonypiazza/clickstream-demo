# ==============================================================================
# Base Producer Abstract Class
# ==============================================================================
"""
Base class for event producers.

Producers read events from a source (e.g., CSV file) and publish them
to a message broker (e.g., Kafka). Each framework implements its own
producer using its native APIs.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseProducer(ABC):
    """Base class for event producers."""

    @abstractmethod
    def setup(self) -> None:
        """
        Initialize producer resources.

        Called once before producing events.
        Use this to establish connections, ensure topics exist, etc.
        """
        ...

    @abstractmethod
    def produce(self, events: Any, **options) -> int:
        """
        Produce events.

        Args:
            events: Events to produce (iterable, file path, etc.)
            **options: Framework-specific options (limit, speed, etc.)

        Returns:
            Count of events produced
        """
        ...

    @abstractmethod
    def cleanup(self) -> None:
        """
        Release producer resources.

        Called when production is complete or interrupted.
        Use this to flush buffers, close connections, etc.
        """
        ...
