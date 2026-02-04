# ==============================================================================
# OpenSearch Sink for Bytewax
# ==============================================================================
"""
OpenSearch sink for Bytewax dataflows.

Thin adapter that wraps OpenSearchRepository and handles
Bytewax-specific concerns (batch processing, connection management).
"""

import logging
from typing import List, Optional

from bytewax.outputs import DynamicSink, StatelessSinkPartition
from opensearchpy.exceptions import ConnectionError as OSConnectionError
from opensearchpy.exceptions import ConnectionTimeout

from clickstream.infrastructure.search import OpenSearchRepository
from clickstream.utils.config import Settings, get_settings

logger = logging.getLogger(__name__)


class OpenSearchEventPartition(StatelessSinkPartition):
    """
    Partition handler for OpenSearch event sink.

    Handles batch indexing of events to OpenSearch with connection
    error recovery.
    """

    def __init__(self, settings: Settings):
        """
        Initialize the partition.

        Args:
            settings: Application settings
        """
        self._repo = OpenSearchRepository(settings)
        self._repo.connect()

    def write_batch(self, items: List[dict]) -> None:
        """
        Write a batch of events to OpenSearch.

        Args:
            items: List of event dictionaries
        """
        if not items:
            return

        try:
            self._repo.save(items)

        except (OSConnectionError, ConnectionTimeout) as e:
            logger.warning("Connection error, will retry: %s", e)
            # OpenSearch repository handles retries internally
            raise

        except Exception as e:
            logger.error("Failed to index events: %s", e)
            raise

    def close(self) -> None:
        """Close the connection."""
        self._repo.close()


class OpenSearchEventSink(DynamicSink):
    """
    Bytewax sink for bulk-indexing events to OpenSearch.

    Wraps OpenSearchRepository and creates partition handlers
    for each worker.
    """

    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialize the sink.

        Args:
            settings: Application settings. If None, uses get_settings().
        """
        self._settings = settings or get_settings()

    def build(self, step_id: str, worker_index: int, worker_count: int) -> StatelessSinkPartition:
        """
        Build a partition handler for this worker.

        Args:
            step_id: Unique step identifier
            worker_index: Index of this worker
            worker_count: Total number of workers

        Returns:
            OpenSearchEventPartition instance
        """
        return OpenSearchEventPartition(self._settings)
