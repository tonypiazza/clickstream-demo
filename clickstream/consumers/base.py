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

    The consumer implementation controls its own parallelism model:
    - Traditional consumers (confluent, kafka_python): one process per partition
    - Dataflow consumers (bytewax): single process with internal workers
    """

    def __init__(self, consumer_type: ConsumerType, num_partitions: int):
        """
        Initialize the consumer.

        Args:
            consumer_type: Type of consumer ("postgresql" or "opensearch")
            num_partitions: Number of Kafka partitions for the topic
        """
        if consumer_type not in ("postgresql", "opensearch"):
            raise ValueError(f"Unknown consumer type: {consumer_type}")
        self._consumer_type: ConsumerType = consumer_type  # type: ignore[assignment]
        self._num_partitions = num_partitions

    @property
    def consumer_type(self) -> ConsumerType:
        """Return the consumer type."""
        return self._consumer_type

    @property
    def num_partitions(self) -> int:
        """Return the number of Kafka partitions."""
        return self._num_partitions

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Human-readable consumer name.

        Examples:
            'kafka-python'
            'quix-streams'
            'mage-ai'
            'bytewax'
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

    @property
    def num_instances(self) -> int:
        """
        Number of OS processes to start for this consumer.

        Default: one process per partition (traditional consumer group model).
        Override in subclass for different parallelism models (e.g., Bytewax).

        Returns:
            Number of processes to spawn.
        """
        return self._num_partitions

    @property
    def num_workers(self) -> int:
        """
        Total worker count across all processes.

        Default: equal to num_instances (one worker per process).
        Override in subclass if a single process runs multiple workers.

        Returns:
            Total number of workers processing partitions.
        """
        return self._num_partitions

    @property
    def parallelism_description(self) -> str:
        """
        Human-readable description of parallelism for CLI output.

        Returns:
            Description like "3 processes" or "1 process x 3 workers"
        """
        if self.num_instances == self.num_workers:
            if self.num_instances == 1:
                return "1 process"
            return f"{self.num_instances} processes"
        else:
            if self.num_instances == 1:
                return f"1 process x {self.num_workers} workers"
            return f"{self.num_instances} processes x {self.num_workers} workers"

    @abstractmethod
    def run(self) -> None:
        """Run the consumer."""
        pass
