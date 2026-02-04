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

    def __init__(self, settings: Settings):
        """
        Initialize the partition.

        Args:
            settings: Application settings
        """
        self._repo = PostgreSQLEventRepository(settings)
        self._repo.connect()

    def write_batch(self, items: List[dict]) -> None:
        """
        Write a batch of events to PostgreSQL.

        Args:
            items: List of event dictionaries
        """
        if not items:
            return

        try:
            self._repo.save(items)

        except psycopg2.OperationalError as e:
            logger.warning("Connection error, reconnecting: %s", e)
            self._repo.rollback()
            self._repo.reconnect()
            # Retry once after reconnection
            self._repo.save(items)

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
            PostgreSQLEventPartition instance
        """
        return PostgreSQLEventPartition(self._settings)


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

    def write_batch(self, items: List[dict]) -> None:
        """
        Write a batch of events as sessions to PostgreSQL.

        Includes retry logic for deadlocks, which can occur when multiple
        consumer processes attempt to upsert the same session_id simultaneously.

        Args:
            items: List of event dictionaries
        """
        if not items:
            return

        # Batch update sessions in Valkey (2 round-trips total)
        updated_sessions = self._session_state.batch_update_sessions(items)
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


class PostgreSQLPartition(StatelessSinkPartition):
    """
    Unified partition handler for PostgreSQL events and sessions.

    Handles both events and sessions in a single write_batch() call,
    matching the pattern used by kafka_python and confluent consumers:
    1. Save events to PostgreSQL
    2. Batch update sessions in Valkey
    3. Save sessions to PostgreSQL

    This eliminates the dual-sink overhead that caused poor performance.
    """

    def __init__(self, settings: Settings, session_state: "SessionState"):
        """
        Initialize the partition.

        Args:
            settings: Application settings
            session_state: SessionState instance for Valkey operations
        """
        self._event_repo = PostgreSQLEventRepository(settings)
        self._event_repo.connect()
        self._session_repo = PostgreSQLSessionRepository(settings)
        self._session_repo.connect()
        self._session_state = session_state

    def write_batch(self, items: List[dict]) -> None:
        """
        Write a batch of events and sessions to PostgreSQL.

        Processes both events and sessions in a single coordinated flow,
        with error handling for connection issues and deadlocks.

        Args:
            items: List of event dictionaries
        """
        if not items:
            return

        try:
            # 1. Save events to PostgreSQL
            self._event_repo.save(items)

            # 2. Batch update sessions in Valkey (2 round-trips total)
            updated_sessions = self._session_state.batch_update_sessions(items)

            # 3. Save sessions to PostgreSQL (with deadlock retry)
            if updated_sessions:
                session_records = [self._session_state.to_db_record(s) for s in updated_sessions]
                self._save_sessions_with_retry(session_records)

        except psycopg2.OperationalError as e:
            logger.warning("Connection error, reconnecting: %s", e)
            self._event_repo.rollback()
            self._session_repo.rollback()
            self._reconnect()
            # Retry once after reconnection
            self._retry_batch(items)

        except Exception as e:
            logger.error("Failed to process batch: %s", e)
            self._event_repo.rollback()
            self._session_repo.rollback()
            raise

    def _save_sessions_with_retry(self, sessions: List[dict]) -> None:
        """
        Save sessions with deadlock retry logic.

        Args:
            sessions: List of session records to save
        """
        last_error = None
        for attempt in range(MAX_DEADLOCK_RETRIES):
            try:
                self._session_repo.save(sessions)
                return  # Success

            except pg_errors.TransactionRollbackError as e:
                last_error = e
                self._session_repo.rollback()

                if attempt < MAX_DEADLOCK_RETRIES - 1:
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

        if last_error:
            raise last_error

    def _reconnect(self) -> None:
        """Reconnect both repositories."""
        self._event_repo.reconnect()
        self._session_repo.reconnect()

    def _retry_batch(self, items: List[dict]) -> None:
        """
        Retry processing a batch after reconnection.

        Args:
            items: List of event dictionaries to retry
        """
        try:
            self._event_repo.save(items)
            updated_sessions = self._session_state.batch_update_sessions(items)
            if updated_sessions:
                session_records = [self._session_state.to_db_record(s) for s in updated_sessions]
                self._session_repo.save(session_records)
        except Exception as e:
            logger.error("Failed to process batch after reconnect: %s", e)
            self._event_repo.rollback()
            self._session_repo.rollback()
            raise

    def close(self) -> None:
        """Close both repository connections."""
        self._event_repo.close()
        self._session_repo.close()


class PostgreSQLSink(DynamicSink):
    """
    Unified Bytewax sink for events and sessions.

    Processes both events and sessions in a single write_batch() call,
    eliminating the dual-sink overhead that caused poor performance.

    This matches the processing pattern used by kafka_python and confluent
    consumers for optimal throughput.
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
            PostgreSQLPartition instance

        Raises:
            RuntimeError: If SessionState is not configured
        """
        if self._session_state is None:
            raise RuntimeError("SessionState not configured")
        return PostgreSQLPartition(self._settings, self._session_state)
