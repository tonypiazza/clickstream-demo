# ==============================================================================
# PostgreSQL Sinks for Bytewax
# ==============================================================================
"""
PostgreSQL sinks for Bytewax dataflows.

Thin adapters that wrap infrastructure repositories and handle
Bytewax-specific concerns (batch processing, connection management).
"""

import logging
from typing import TYPE_CHECKING, Optional

import psycopg2
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from clickstream.consumers.batch_processor import BatchMetrics
from clickstream.infrastructure.repositories import (
    PostgreSQLEventRepository,
    PostgreSQLSessionRepository,
)
from clickstream.utils.config import Settings, get_settings

if TYPE_CHECKING:
    from clickstream.utils.session_state import SessionState

logger = logging.getLogger(__name__)


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
        self._batch_metrics = BatchMetrics(
            self._event_repo,
            self._session_state,
            self._session_repo,
            log=logger,
        )

    def write_batch(self, items: list[dict]) -> None:
        """
        Write a batch of events and sessions to PostgreSQL.

        Processes both events and sessions in a single coordinated flow,
        with error handling for connection issues.

        Args:
            items: List of event dictionaries
        """
        if not items:
            return

        try:
            self._batch_metrics.process_batch(items)

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

    def _reconnect(self) -> None:
        """Reconnect both repositories."""
        self._event_repo.reconnect()
        self._session_repo.reconnect()

    def _retry_batch(self, items: list[dict]) -> None:
        """
        Retry processing a batch after reconnection.

        Args:
            items: List of event dictionaries to retry
        """
        try:
            self._batch_metrics.process_batch(items)
        except Exception as e:
            logger.error("Failed to process batch after reconnect: %s", e)
            self._event_repo.rollback()
            self._session_repo.rollback()
            raise

    def close(self) -> None:
        """Close both repository connections and log final summary."""
        self._batch_metrics.log_final_summary()
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
        settings: Settings | None = None,
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
