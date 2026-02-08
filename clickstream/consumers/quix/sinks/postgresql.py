"""
PostgreSQL sinks for Quix Streams.

Thin adapters that wrap infrastructure repositories and handle
Quix-specific concerns (batching, backpressure).
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

import psycopg2
from quixstreams.sinks.base import BatchingSink, SinkBackpressureError, SinkBatch

from clickstream.consumers.batch_processor import BatchMetrics
from clickstream.infrastructure.repositories import (
    PostgreSQLEventRepository,
    PostgreSQLSessionRepository,
)
from clickstream.utils.config import Settings, get_settings
from clickstream.utils.retry import RETRY_WAIT_MAX

if TYPE_CHECKING:
    from clickstream.utils.session_state import SessionState

logger = logging.getLogger(__name__)


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
        settings: Settings | None = None,
        session_state: Optional["SessionState"] = None,
        consumer: Any | None = None,
    ):
        super().__init__()
        self._settings = settings or get_settings()
        self._event_repo = PostgreSQLEventRepository(self._settings)
        self._session_repo = PostgreSQLSessionRepository(self._settings)
        self._session_state = session_state
        self._consumer = consumer  # Quix InternalConsumer for lag monitoring
        self._batch_metrics: BatchMetrics | None = None

    def setup(self):
        """Called once when the sink starts."""
        self._event_repo.connect()
        self._session_repo.connect()

        # Create batch metrics after repos are connected
        self._batch_metrics = BatchMetrics(
            self._event_repo,
            self._session_state,
            self._session_repo,
            on_summary=self._log_consumer_lag,
            log=logger,
        )

    def write(self, batch: SinkBatch):
        """Write a batch of events and sessions to PostgreSQL."""
        if self._session_state is None:
            raise RuntimeError("SessionState not configured")
        if self._batch_metrics is None:
            raise RuntimeError("Sink not set up. Call setup() first.")

        events = [item.value for item in batch]
        if not events:
            return

        try:
            # Process batch with instrumented 3-step pipeline
            self._batch_metrics.process_batch(events)

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

    def _log_consumer_lag(self) -> None:
        """Log consumer lag using the injected Quix consumer."""
        if self._consumer is None:
            return
        try:

            assignment = self._consumer.assignment()
            if not assignment:
                return

            parts = []
            total_lag = 0
            for tp in sorted(assignment, key=lambda tp: tp.partition):
                try:
                    _, high = self._consumer.get_watermark_offsets(tp, cached=True)
                    pos = self._consumer.position([tp])
                    if pos and pos[0].offset >= 0:
                        lag = max(0, high - pos[0].offset)
                    else:
                        lag = -1
                except Exception:
                    lag = -1
                if lag >= 0:
                    parts.append(f"p{tp.partition}={lag:,}")
                    total_lag += lag
                else:
                    parts.append(f"p{tp.partition}=?")
            logger.info("Consumer lag: %s | total=%s", " ".join(parts), f"{total_lag:,}")
        except Exception as e:
            logger.debug("Could not log consumer lag: %s", e)

    def _reconnect(self) -> None:
        """Reconnect both repositories."""
        self._event_repo.reconnect()
        self._session_repo.reconnect()

    def cleanup(self):
        """Called when the sink is being shut down."""
        if self._batch_metrics:
            self._batch_metrics.log_final_summary()
        self._event_repo.close()
        self._session_repo.close()
