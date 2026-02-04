# ==============================================================================
# Streaming Producer Base Class
# ==============================================================================
"""
Abstract base class for streaming producer implementations.

Producers handle publishing events to Kafka. Available implementations:
- kafka_python: Uses kafka-python library directly
- quix: Uses Quix Streams producer API
"""

from abc import ABC, abstractmethod
from typing import Optional


class StreamingProducer(ABC):
    """
    Abstract base for streaming producer implementations.

    Each implementation handles publishing events to Kafka using its
    respective library (kafka-python or Quix Streams).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Human-readable producer name (matches pip package name).

        Examples:
            'kafka-python'
            'quixstreams'
            'confluent-kafka'
        """
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """
        Return version string for the producer implementation.

        Format: "{package} v{version}"
        Example: "kafka-python v2.0.2" or "quixstreams v3.0.0"
        """
        pass

    @abstractmethod
    def run(
        self,
        limit: Optional[int] = None,
        realtime: bool = False,
        speed: float = 1.0,
    ) -> None:
        """
        Run the producer.

        Args:
            limit: Maximum number of events to produce (None for all)
            realtime: Whether to replay events in real-time
            speed: Speed multiplier for real-time replay (e.g., 2.0 for 2x speed)
        """
        pass
