# ==============================================================================
# Streaming Framework Base Classes
# ==============================================================================
"""
Abstract base classes for streaming framework implementations.

Each framework (Quix, Mage, SDK) must implement these interfaces.
The framework abstraction allows switching between different streaming
implementations via the STREAMING_IMPL environment variable.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class StreamingProducer(ABC):
    """
    Abstract base for streaming producer implementations.

    Each framework provides a producer that handles publishing events to Kafka.
    The producer can use framework-specific features or fall back to kafka-python.
    """

    @property
    @abstractmethod
    def version(self) -> str:
        """
        Return version string for the producer implementation.

        Format: "{Framework Name} v{version}" or "kafka-python v{version}"
        Example: "Quix Streams v3.0.0" or "kafka-python v2.0.2"
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


class StreamingConsumer(ABC):
    """
    Abstract base for streaming consumer implementations.

    Each framework provides consumers that handle reading from Kafka and
    writing to PostgreSQL or OpenSearch.
    """

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


class StreamingFramework(ABC):
    """
    Abstract base for streaming framework implementations.

    Frameworks handle:
    - Consumer lifecycle (start, run, stop)
    - Producer lifecycle (start, run, stop)
    - Framework-specific configuration and project paths
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable framework name."""
        pass

    @property
    @abstractmethod
    def project_path(self) -> Path:
        """Path to framework-specific project files."""
        pass

    @abstractmethod
    def get_producer(self) -> StreamingProducer:
        """
        Get the producer instance for this framework.

        Returns:
            StreamingProducer: Producer instance with run() and version
        """
        pass

    @abstractmethod
    def get_consumer(self, consumer_type: str) -> StreamingConsumer:
        """
        Get a consumer instance for this framework.

        Args:
            consumer_type: Type of consumer ("postgresql" or "opensearch")

        Returns:
            StreamingConsumer: Consumer instance with run() and version
        """
        pass

    def get_consumer_runner_module(self, consumer_type: str) -> str:
        """
        Get the module path for the consumer runner script.

        Args:
            consumer_type: Type of consumer ("postgresql" or "opensearch")

        Returns:
            Module path to the runner script (e.g., "clickstream.consumer_runner")
        """
        if consumer_type not in ("postgresql", "opensearch"):
            raise ValueError(f"Unknown consumer type: {consumer_type}")
        return "clickstream.consumer_runner"

    def get_producer_runner_module(self) -> str:
        """
        Get the module path for the producer runner script.

        Returns:
            Module path to the runner script
        """
        return "clickstream.producer_runner"
