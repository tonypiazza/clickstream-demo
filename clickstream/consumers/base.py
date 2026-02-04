# ==============================================================================
# Streaming Consumer Base Class
# ==============================================================================
"""
Abstract base class for streaming consumer implementations.

Each consumer implementation (kafka_python, quix, mage, bytewax) must
implement this interface.
"""

from abc import ABC, abstractmethod
from typing import Literal

# Consumer types supported by all implementations
ConsumerType = Literal["postgresql", "opensearch"]


class StreamingConsumer(ABC):
    """
    Abstract base for streaming consumer implementations.

    Each implementation provides consumers that handle reading from Kafka and
    writing to PostgreSQL or OpenSearch.
    """

    def __init__(self, consumer_type: ConsumerType):
        """
        Initialize the consumer.

        Args:
            consumer_type: Type of consumer ("postgresql" or "opensearch")
        """
        if consumer_type not in ("postgresql", "opensearch"):
            raise ValueError(f"Unknown consumer type: {consumer_type}")
        self._consumer_type = consumer_type

    @property
    def consumer_type(self) -> ConsumerType:
        """Return the consumer type."""
        return self._consumer_type

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Human-readable consumer name.

        Examples:
            'kafka-python'
            'Quix Streams'
            'Mage AI'
            'Bytewax'
        """
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """
        Return version string for the consumer implementation.

        Format: "{Framework Name} v{version}"
        Example: "Quix Streams v3.0.0" or "Mage AI v0.9.73"
        """
        pass

    @abstractmethod
    def run(self) -> None:
        """Run the consumer."""
        pass
