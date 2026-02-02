# ==============================================================================
# PostgreSQL Sinks for Bytewax
# ==============================================================================
"""
PostgreSQL sinks for Bytewax dataflows.

Thin adapters that wrap infrastructure repositories and handle
Bytewax-specific concerns (batch processing, connection management).
"""

import logging
import random
import time
from typing import TYPE_CHECKING, List, Optional

import psycopg2
from psycopg2 import errors as pg_errors
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from clickstream.infrastructure.metrics import set_last_message_timestamp
from clickstream.infrastructure.repositories import (
    PostgreSQLEventRepository,
    PostgreSQLSessionRepository,
)
from clickstream.utils.config import Settings, get_settings

if TYPE_CHECKING:
    from clickstream.utils.session_state import SessionState

logger = logging.getLogger(__name__)

# Deadlock retry configuration
MAX_DEADLOCK_RETRIES = 5
BASE_RETRY_DELAY = 0.05  # 50ms base delay


class PostgreSQLEventPartition(StatelessSinkPartition):
    """
    Partition handler for PostgreSQL event sink.

    Handles batch writes of events to PostgreSQL with connection
    error recovery.
    """

    def __init__(self, settings: Settings, group_id: str):
        """
        Initialize the partition.

        Args:
            settings: Application settings
            group_id: Consumer group ID for metrics tracking
        """
        self._repo = PostgreSQLEventRepository(settings)
        self._repo.connect()
        self._group_id = group_id

    def write_batch(self, batch: List[dict]) -> None:
        """
        Write a batch of events to PostgreSQL.

        Args:
            batch: List of event dictionaries
        """
        if not batch:
            return

        try:
            self._repo.save(batch)

            if self._group_id:
                set_last_message_timestamp(self._group_id)

        except psycopg2.OperationalError as e:
            logger.warning("Connection error, reconnecting: %s", e)
            self._repo.rollback()
            self._repo.reconnect()
            # Retry once after reconnection
            self._repo.save(batch)

        except Exception as e:
            logger.error("Failed to insert events: %s", e)
            self._repo.rollback()
            raise

    def close(self) -> None:
        """Close the connection."""
        self._repo.close()


class PostgreSQLEventSink(DynamicSink):
    """
    Bytewax sink for bulk-inserting events to PostgreSQL.

    Wraps PostgreSQLEventRepository and creates partition handlers
    for each worker.
    """

    def __init__(self, settings: Optional[Settings] = None, group_id: str = ""):
        """
        Initialize the sink.

        Args:
            settings: Application settings. If None, uses get_settings().
            group_id: Consumer group ID for metrics tracking
        """
        self._settings = settings or get_settings()
        self._group_id = group_id

    def build(self, step_id: str, worker_index: int, worker_count: int) -> StatelessSinkPartition:
        """
        Build a partition handler for this worker.

        Args:
            step_id: Unique step identifier
            worker_index: Index of this worker
            worker_count: Total number of workers

        Returns:
            PostgreSQLEventPartition instance
        """
        return PostgreSQLEventPartition(self._settings, self._group_id)


class PostgreSQLSessionPartition(StatelessSinkPartition):
    """
    Partition handler for PostgreSQL session sink.

    Handles batch updates of sessions:
    1. Batch update sessions in Valkey
    2. Upsert sessions to PostgreSQL
    """

    def __init__(self, settings: Settings, session_state: "SessionState"):
        """
        Initialize the partition.

        Args:
            settings: Application settings
            session_state: SessionState instance for Valkey operations
        """
        self._repo = PostgreSQLSessionRepository(settings)
        self._repo.connect()
        self._session_state = session_state

    def write_batch(self, batch: List[dict]) -> None:
        """
        Write a batch of events as sessions to PostgreSQL.

        Includes retry logic for deadlocks, which can occur when multiple
        consumer processes attempt to upsert the same session_id simultaneously.

        Args:
            batch: List of event dictionaries
        """
        if not batch:
            return

        # Batch update sessions in Valkey (2 round-trips total)
        updated_sessions = self._session_state.batch_update_sessions(batch)
        if not updated_sessions:
            return

        # Convert to DB records
        sessions = [self._session_state.to_db_record(s) for s in updated_sessions]

        # Retry loop for deadlock handling
        last_error = None
        for attempt in range(MAX_DEADLOCK_RETRIES):
            try:
                self._repo.save(sessions)
                return  # Success - exit the retry loop

            except pg_errors.TransactionRollbackError as e:
                # Includes DeadlockDetected and SerializationFailure
                last_error = e
                self._repo.rollback()

                if attempt < MAX_DEADLOCK_RETRIES - 1:
                    # Exponential backoff with jitter
                    delay = BASE_RETRY_DELAY * (2**attempt) + random.uniform(0, 0.1)
                    logger.warning(
                        "Deadlock detected on session upsert (attempt %d/%d), "
                        "retrying in %.3fs: %s",
                        attempt + 1,
                        MAX_DEADLOCK_RETRIES,
                        delay,
                        e,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "Deadlock persisted after %d retries: %s",
                        MAX_DEADLOCK_RETRIES,
                        e,
                    )

            except psycopg2.OperationalError as e:
                logger.warning("Connection error, reconnecting: %s", e)
                self._repo.rollback()
                self._repo.reconnect()
                # Retry once after reconnection
                try:
                    self._repo.save(sessions)
                    return
                except Exception as retry_error:
                    logger.error("Failed to save sessions after reconnect: %s", retry_error)
                    self._repo.rollback()
                    raise

            except Exception as e:
                logger.error("Failed to upsert sessions: %s", e)
                self._repo.rollback()
                raise

        # If we exhausted all retries, raise the last error
        if last_error:
            raise last_error

    def close(self) -> None:
        """Close the connection."""
        self._repo.close()


class PostgreSQLSessionSink(DynamicSink):
    """
    Bytewax sink for upserting sessions to PostgreSQL.

    This sink:
    1. Receives raw events from the dataflow
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
        """
        Initialize the sink.

        Args:
            settings: Application settings. If None, uses get_settings().
            session_state: SessionState instance for Valkey operations
        """
        self._settings = settings or get_settings()
        self._session_state = session_state

    def build(self, step_id: str, worker_index: int, worker_count: int) -> StatelessSinkPartition:
        """
        Build a partition handler for this worker.

        Args:
            step_id: Unique step identifier
            worker_index: Index of this worker
            worker_count: Total number of workers

        Returns:
            PostgreSQLSessionPartition instance

        Raises:
            RuntimeError: If SessionState is not configured
        """
        if self._session_state is None:
            raise RuntimeError("SessionState not configured")
        return PostgreSQLSessionPartition(self._settings, self._session_state)
