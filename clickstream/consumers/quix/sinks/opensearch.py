"""
OpenSearch sink for Quix Streams.

Thin adapter that wraps OpenSearchRepository and handles
Quix-specific concerns (batching, backpressure).
"""

import logging
from typing import Optional

from opensearchpy.exceptions import ConnectionError as OSConnectionError
from opensearchpy.exceptions import ConnectionTimeout
from quixstreams.sinks.base import BatchingSink, SinkBackpressureError, SinkBatch

from clickstream.infrastructure.search import OpenSearchRepository
from clickstream.utils.config import Settings, get_settings
from clickstream.utils.retry import RETRY_WAIT_MAX

logger = logging.getLogger(__name__)


class OpenSearchEventSink(BatchingSink):
    """
    Quix sink for bulk-indexing events to OpenSearch.

    Wraps OpenSearchRepository and handles Quix-specific concerns:
    - Extracting events from SinkBatch
    - Raising SinkBackpressureError on connection errors
    """

    def __init__(self, settings: Optional[Settings] = None):
        super().__init__()
        self._settings = settings or get_settings()
        self._repo = OpenSearchRepository(self._settings)

    def setup(self):
        """Called once when the sink starts."""
        self._repo.connect()

    def write(self, batch: SinkBatch):
        """Write a batch of events to OpenSearch."""
        events = [item.value for item in batch]
        if not events:
            return

        try:
            self._repo.save(events)

        except (OSConnectionError, ConnectionTimeout) as e:
            logger.warning("Connection error, requesting backpressure: %s", e)
            raise SinkBackpressureError(retry_after=RETRY_WAIT_MAX)

        except Exception as e:
            logger.error("Failed to index events: %s", e)
            raise

    def cleanup(self):
        """Called when the sink is being shut down."""
        self._repo.close()
