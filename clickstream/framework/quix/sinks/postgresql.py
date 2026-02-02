"""
PostgreSQL sinks for Quix Streams.

Thin adapters that wrap infrastructure repositories and handle
Quix-specific concerns (batching, backpressure).
"""

import logging
from typing import TYPE_CHECKING, Optional

import psycopg2
from quixstreams.sinks.base import BatchingSink, SinkBackpressureError, SinkBatch

from clickstream.infrastructure.metrics import set_last_message_timestamp
from clickstream.infrastructure.repositories import (
    PostgreSQLEventRepository,
    PostgreSQLSessionRepository,
)
from clickstream.utils.config import Settings, get_settings
from clickstream.utils.retry import RETRY_WAIT_MAX

if TYPE_CHECKING:
    from clickstream.utils.session_state import SessionState

logger = logging.getLogger(__name__)


class PostgreSQLEventSink(BatchingSink):
    """
    Quix sink for bulk-inserting events to PostgreSQL.

    Wraps PostgreSQLEventRepository and handles Quix-specific concerns:
    - Extracting events from SinkBatch
    - Raising SinkBackpressureError on connection errors
    - Updating last message timestamp for metrics
    """

    def __init__(self, settings: Optional[Settings] = None, group_id: Optional[str] = None):
        super().__init__()
        self._settings = settings or get_settings()
        self._repo = PostgreSQLEventRepository(self._settings)
        self._group_id = group_id

    def setup(self):
        """Called once when the sink starts."""
        self._repo.connect()

    def write(self, batch: SinkBatch):
        """Write a batch of events to PostgreSQL."""
        events = [item.value for item in batch]
        if not events:
            return

        try:
            self._repo.save(events)

            if self._group_id:
                set_last_message_timestamp(self._group_id)

        except psycopg2.OperationalError as e:
            logger.warning("Connection error, requesting backpressure: %s", e)
            self._repo.rollback()
            self._repo.reconnect()
            raise SinkBackpressureError(retry_after=RETRY_WAIT_MAX)

        except Exception as e:
            logger.error("Failed to insert events: %s", e)
            self._repo.rollback()
            raise

    def cleanup(self):
        """Called when the sink is being shut down."""
        self._repo.close()


class PostgreSQLSessionSink(BatchingSink):
    """
    Quix sink for upserting sessions to PostgreSQL.

    This sink:
    1. Receives raw events from Kafka
    2. Batches Valkey session state updates (2 round-trips per batch)
    3. Upserts resulting sessions to PostgreSQL

    Wraps PostgreSQLSessionRepository for persistence and uses
    SessionState for session aggregation logic.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        session_state: Optional["SessionState"] = None,
    ):
        super().__init__()
        self._settings = settings or get_settings()
        self._repo = PostgreSQLSessionRepository(self._settings)
        self._session_state = session_state

    def setup(self):
        """Called once when the sink starts."""
        self._repo.connect()

    def write(self, batch: SinkBatch):
        """Write a batch of events to PostgreSQL as sessions."""
        if self._session_state is None:
            raise RuntimeError("SessionState not configured")

        events = [item.value for item in batch]
        if not events:
            return

        # Batch update sessions in Valkey (2 round-trips total)
        updated_sessions = self._session_state.batch_update_sessions(events)
        if not updated_sessions:
            return

        # Convert to DB records
        sessions = [self._session_state.to_db_record(s) for s in updated_sessions]

        try:
            self._repo.save(sessions)

        except psycopg2.OperationalError as e:
            logger.warning("Connection error, requesting backpressure: %s", e)
            self._repo.rollback()
            self._repo.reconnect()
            raise SinkBackpressureError(retry_after=RETRY_WAIT_MAX)

        except Exception as e:
            logger.error("Failed to upsert sessions: %s", e)
            self._repo.rollback()
            raise

    def cleanup(self):
        """Called when the sink is being shut down."""
        self._repo.close()


# Backward compatibility aliases (deprecated)
PostgreSQLEventsSink = PostgreSQLEventSink
PostgreSQLSessionsSink = PostgreSQLSessionSink
