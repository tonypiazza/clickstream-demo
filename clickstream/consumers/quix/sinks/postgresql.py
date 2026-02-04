"""
PostgreSQL sinks for Quix Streams.

Thin adapters that wrap infrastructure repositories and handle
Quix-specific concerns (batching, backpressure).
"""

import logging
from typing import TYPE_CHECKING, Optional

import psycopg2
from quixstreams.sinks.base import BatchingSink, SinkBackpressureError, SinkBatch

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
    """

    def __init__(self, settings: Optional[Settings] = None):
        super().__init__()
        self._settings = settings or get_settings()
        self._repo = PostgreSQLEventRepository(self._settings)

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


class PostgreSQLSink(BatchingSink):
    """
    Unified Quix sink for events and sessions.

    Processes both events and sessions in a single write() call,
    eliminating the dual-sink pattern that caused checkpoint/commit issues.

    Processing order:
    1. Save events to PostgreSQL
    2. Batch update sessions in Valkey
    3. Save sessions to PostgreSQL (with retry on failure)
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        session_state: Optional["SessionState"] = None,
    ):
        super().__init__()
        self._settings = settings or get_settings()
        self._event_repo = PostgreSQLEventRepository(self._settings)
        self._session_repo = PostgreSQLSessionRepository(self._settings)
        self._session_state = session_state

    def setup(self):
        """Called once when the sink starts."""
        self._event_repo.connect()
        self._session_repo.connect()

    def write(self, batch: SinkBatch):
        """Write a batch of events and sessions to PostgreSQL."""
        if self._session_state is None:
            raise RuntimeError("SessionState not configured")

        events = [item.value for item in batch]
        if not events:
            return

        try:
            # 1. Save events to PostgreSQL
            self._event_repo.save(events)

            # 2. Batch update sessions in Valkey (2 round-trips total)
            updated_sessions = self._session_state.batch_update_sessions(events)

            # 3. Save sessions to PostgreSQL (with retry on failure)
            if updated_sessions:
                sessions = [self._session_state.to_db_record(s) for s in updated_sessions]
                self._save_sessions_with_retry(sessions)

        except psycopg2.OperationalError as e:
            logger.warning("Connection error, requesting backpressure: %s", e)
            self._event_repo.rollback()
            self._session_repo.rollback()
            self._reconnect()
            raise SinkBackpressureError(retry_after=RETRY_WAIT_MAX)

        except Exception as e:
            logger.error("Failed to process batch: %s", e)
            self._event_repo.rollback()
            self._session_repo.rollback()
            raise

    def _save_sessions_with_retry(self, sessions: list) -> None:
        """Save sessions with retry logic for transient failures."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._session_repo.save(sessions)
                return  # Success
            except psycopg2.OperationalError as e:
                self._session_repo.rollback()
                if attempt < max_retries - 1:
                    logger.warning(
                        "Session upsert failed (attempt %d/%d), retrying: %s",
                        attempt + 1,
                        max_retries,
                        e,
                    )
                    self._session_repo.reconnect()
                else:
                    logger.error(
                        "Session upsert failed after %d retries: %s",
                        max_retries,
                        e,
                    )
                    # Don't raise - events were saved, just log the session failure

    def _reconnect(self) -> None:
        """Reconnect both repositories."""
        self._event_repo.reconnect()
        self._session_repo.reconnect()

    def cleanup(self):
        """Called when the sink is being shut down."""
        self._event_repo.close()
        self._session_repo.close()


# Backward compatibility aliases (deprecated)
PostgreSQLEventsSink = PostgreSQLEventSink
PostgreSQLSessionsSink = PostgreSQLSessionSink
