# ==============================================================================
# Base Consumer Abstract Class
# ==============================================================================
"""
Base class for stream consumers.

Consumers read from a message source (e.g., Kafka) and process records.
Each framework has different consumption patterns, so this ABC is intentionally loose.
"""

from abc import ABC, abstractmethod


class BaseConsumer(ABC):
    """Base class for stream consumers."""

    @abstractmethod
    def setup(self) -> None:
        """
        Initialize consumer resources.

        Called once before the consumer starts processing.
        Use this to establish connections, create topics, etc.
        """
        ...

    @abstractmethod
    def run(self) -> None:
        """
        Run the consumer loop.

        This method should block until shutdown is requested.
        """
        ...

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the consumer gracefully.

        Signal the consumer to stop processing and exit the run loop.
        Should allow in-flight batches to complete.
        """
        ...
