# ==============================================================================
# Base Sink Abstract Class
# ==============================================================================
"""
Base class for stream sinks.

Sinks receive batches of records from a streaming framework and persist them.
Each framework may have different batch representations, so the write()
method accepts a generic batch parameter.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseSink(ABC):
    """Base class for stream sinks."""

    @abstractmethod
    def setup(self) -> None:
        """
        Initialize sink resources.

        Called once before the sink starts receiving data.
        Use this to establish connections, initialize state, etc.
        """
        ...

    @abstractmethod
    def write(self, batch: Any) -> None:
        """
        Write a batch of records.

        Args:
            batch: Framework-specific batch representation
                   (e.g., Quix SinkBatch, list of dicts, etc.)
        """
        ...

    @abstractmethod
    def cleanup(self) -> None:
        """
        Release sink resources.

        Called when the sink is shutting down.
        Use this to close connections, flush buffers, etc.
        """
        ...
