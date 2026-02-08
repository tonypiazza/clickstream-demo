# ==============================================================================
# Shared Batch Processor with Performance Instrumentation
# ==============================================================================
"""
Shared instrumentation for the 3-step PostgreSQL batch processing pipeline.

All 5 consumer implementations (confluent, kafka-python, mage, quix, bytewax)
perform the identical 3-step inner pipeline:

    1. event_repo.save(events)                    - PostgreSQL inserts
    2. session_state.batch_update_sessions(events) - Valkey round-trips
    3. session_repo.save(session_records)          - PostgreSQL session upserts

This module provides a BatchMetrics composition object that wraps these steps
with time.monotonic() timing and provides:

- Per-batch INFO log with stage-level timings
- Periodic throughput summary (configurable interval, default 30s)
- Cumulative stats tracking
- Final summary on shutdown

Usage (self-managed consumers):
    batch_metrics = BatchMetrics(event_repo, session_state, session_repo)
    session_records = batch_metrics.process_batch(events)
    consumer.commit()

Usage (framework-managed sinks):
    self._batch_metrics = BatchMetrics(event_repo, session_state, session_repo)
    session_records = self._batch_metrics.process_batch(events)
"""

import logging
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)


class BatchMetrics:
    """
    Shared instrumentation for the 3-step PostgreSQL batch pipeline.

    Wraps event_repo.save(), session_state.batch_update_sessions(), and
    session_repo.save() with monotonic timing. Tracks cumulative stats
    and logs periodic throughput summaries.

    This is a composition object — each consumer instantiates one and
    delegates batch processing to it. It does NOT handle error recovery
    (rollback, reconnect) — that remains in each consumer since error
    handling varies significantly across implementations.
    """

    def __init__(
        self,
        event_repo,
        session_state,
        session_repo,
        summary_interval_seconds: float = 30.0,
        on_summary: Callable[[], None] | None = None,
        log: logging.Logger | None = None,
        max_poll_interval_ms: int = 300_000,
    ):
        """
        Initialize the batch metrics processor.

        Args:
            event_repo: PostgreSQLEventRepository instance
            session_state: SessionState instance for Valkey operations
            session_repo: PostgreSQLSessionRepository instance
            summary_interval_seconds: How often to log throughput summaries
            on_summary: Optional callback invoked during periodic summaries
                        (e.g., for consumer lag logging from the caller)
            log: Optional logger override. Defaults to this module's logger.
            max_poll_interval_ms: Kafka max.poll.interval.ms for proximity
                                  calculation. Default 300,000 (5 minutes).
        """
        self._event_repo = event_repo
        self._session_state = session_state
        self._session_repo = session_repo
        self._summary_interval = summary_interval_seconds
        self._on_summary = on_summary
        self._log = log or logger

        # Cumulative stats (lifetime of this BatchMetrics instance)
        self._total_events = 0
        self._total_sessions = 0
        self._total_batches = 0
        self._cum_pg_events_ms = 0.0
        self._cum_valkey_ms = 0.0
        self._cum_pg_sessions_ms = 0.0
        self._cum_total_ms = 0.0
        self._start_time = time.monotonic()

        # Period stats (reset each summary interval)
        self._period_events = 0
        self._period_sessions = 0
        self._period_batches = 0
        self._period_pg_events_ms = 0.0
        self._period_valkey_ms = 0.0
        self._period_pg_sessions_ms = 0.0
        self._period_total_ms = 0.0
        self._last_summary_time = time.monotonic()

        # Loop timing stats — populated by record_loop_timing()
        self._cum_poll_ms = 0.0
        self._cum_commit_ms = 0.0
        self._cum_loop_count = 0
        self._cum_fill_sum = 0.0  # sum of fill ratios for averaging

        self._period_poll_ms = 0.0
        self._period_commit_ms = 0.0
        self._period_loop_count = 0
        self._period_fill_ratios: list[float] = []

        # Max poll interval proximity — populated by record_loop_timing()
        self._max_poll_interval_ms = max_poll_interval_ms
        self._last_batch_total_ms = 0.0  # set by process_batch()
        self._period_max_proximity = 0.0  # max proximity seen during period
        self._cum_max_proximity = 0.0  # max proximity seen lifetime

        # Idle time (commit-to-poll gap) — populated by record_loop_timing()
        self._cum_idle_ms = 0.0
        self._period_idle_ms = 0.0

        # Rebalance tracking — populated by record_rebalance()
        self._rebalance_events: list[float] = []

    def process_batch(self, events: list[dict]) -> list[dict]:
        """
        Execute the 3-step pipeline with timing instrumentation.

        Performs:
            1. Save events to PostgreSQL
            2. Batch update sessions in Valkey
            3. Convert and save sessions to PostgreSQL

        Logs per-batch timing at INFO level. Triggers periodic throughput
        summary when the configured interval has elapsed.

        Does NOT handle errors — the caller is responsible for rollback,
        reconnect, and retry logic.

        Args:
            events: List of event dictionaries from Kafka

        Returns:
            List of session record dicts (from to_db_record)

        Raises:
            Any exception from the underlying repos or session_state
        """
        if not events:
            return []

        # 1. Save events to PostgreSQL
        t0 = time.monotonic()
        self._event_repo.save(events)

        # 2. Batch update sessions in Valkey (2 pipeline round-trips)
        t1 = time.monotonic()
        updated_sessions = self._session_state.batch_update_sessions(events)

        # 3. Convert and save sessions to PostgreSQL
        t2 = time.monotonic()
        session_records = [self._session_state.to_db_record(s) for s in updated_sessions]
        self._session_repo.save(session_records)
        t3 = time.monotonic()

        # Calculate per-stage timings (milliseconds)
        pg_events_ms = (t1 - t0) * 1000
        valkey_ms = (t2 - t1) * 1000
        pg_sessions_ms = (t3 - t2) * 1000
        total_ms = (t3 - t0) * 1000

        num_events = len(events)
        num_sessions = len(session_records)

        # Log per-batch timing
        self._log.info(
            "Batch: %s events, %s sessions | "
            "pg_events=%.*fms valkey=%.*fms pg_sessions=%.*fms | "
            "total=%.*fms",
            f"{num_events:,}",
            f"{num_sessions:,}",
            _precision(pg_events_ms),
            pg_events_ms,
            _precision(valkey_ms),
            valkey_ms,
            _precision(pg_sessions_ms),
            pg_sessions_ms,
            _precision(total_ms),
            total_ms,
        )

        # Update cumulative stats
        self._total_events += num_events
        self._total_sessions += num_sessions
        self._total_batches += 1
        self._cum_pg_events_ms += pg_events_ms
        self._cum_valkey_ms += valkey_ms
        self._cum_pg_sessions_ms += pg_sessions_ms
        self._cum_total_ms += total_ms

        # Update period stats
        self._period_events += num_events
        self._period_sessions += num_sessions
        self._period_batches += 1
        self._period_pg_events_ms += pg_events_ms
        self._period_valkey_ms += valkey_ms
        self._period_pg_sessions_ms += pg_sessions_ms
        self._period_total_ms += total_ms

        # Store for proximity calculation in record_loop_timing()
        self._last_batch_total_ms = total_ms

        # Check if periodic summary is due
        now = time.monotonic()
        if now - self._last_summary_time >= self._summary_interval:
            self._log_summary(now)

        return session_records

    def record_loop_timing(
        self,
        poll_ms: float,
        commit_ms: float,
        batch_size: int,
        max_batch_size: int,
        idle_ms: float = 0.0,
    ) -> None:
        """
        Record poll and commit timings from the consumer loop.

        Called by each consumer after commit() to track the full loop cycle
        (poll → process → commit). These timings are included in periodic
        summaries alongside the 3-step pipeline timings.

        Also computes max poll interval proximity: the ratio of the total
        loop time (poll + process + commit) to max.poll.interval.ms. A
        proximity > 0.7 triggers a WARNING; > 0.9 triggers a CRITICAL log.

        Args:
            poll_ms: Time spent in consume()/poll() in milliseconds
            commit_ms: Time spent in commit() in milliseconds
            batch_size: Number of messages returned by this poll
            max_batch_size: Maximum messages requested (num_messages / max_records)
            idle_ms: Gap between previous commit and this poll (headroom)
        """
        fill_ratio = batch_size / max_batch_size if max_batch_size > 0 else 0.0

        self._cum_poll_ms += poll_ms
        self._cum_commit_ms += commit_ms
        self._cum_loop_count += 1
        self._cum_fill_sum += fill_ratio
        self._cum_idle_ms += idle_ms

        self._period_poll_ms += poll_ms
        self._period_commit_ms += commit_ms
        self._period_loop_count += 1
        self._period_fill_ratios.append(fill_ratio)
        self._period_idle_ms += idle_ms

        # Compute max poll interval proximity
        if self._max_poll_interval_ms > 0:
            loop_ms = poll_ms + self._last_batch_total_ms + commit_ms
            proximity = loop_ms / self._max_poll_interval_ms
            self._period_max_proximity = max(self._period_max_proximity, proximity)
            self._cum_max_proximity = max(self._cum_max_proximity, proximity)

            # Threshold warnings
            if proximity > 0.9:
                self._log.critical(
                    "Poll proximity %.2f — consumer may be evicted (loop=%.0fms / max_poll=%dms)",
                    proximity,
                    loop_ms,
                    self._max_poll_interval_ms,
                )
            elif proximity > 0.7:
                self._log.warning(
                    "Poll proximity %.2f — consumer nearing eviction threshold "
                    "(loop=%.0fms / max_poll=%dms)",
                    proximity,
                    loop_ms,
                    self._max_poll_interval_ms,
                )

    def record_rebalance(self) -> None:
        """
        Record a consumer rebalance event.

        Should be called from the consumer's on_assign callback (not
        on_revoke, to avoid double-counting). Timestamps are used to
        detect frequent rebalances in periodic summaries.
        """
        self._rebalance_events.append(time.monotonic())

    def _log_summary(self, now: float) -> None:
        """Log periodic throughput summary and reset period counters."""
        elapsed = now - self._last_summary_time
        if elapsed <= 0 or self._period_batches == 0:
            return

        events_per_sec = self._period_events / elapsed
        avg_batch_ms = self._period_total_ms / self._period_batches
        avg_pg_events_ms = self._period_pg_events_ms / self._period_batches
        avg_valkey_ms = self._period_valkey_ms / self._period_batches
        avg_pg_sessions_ms = self._period_pg_sessions_ms / self._period_batches

        self._log.info(
            "Throughput (%.1fs): %s events/sec | batches=%d | "
            "avg_batch=%.*fms | "
            "avg_pg_events=%.*fms avg_valkey=%.*fms avg_pg_sessions=%.*fms",
            elapsed,
            f"{events_per_sec:,.0f}",
            self._period_batches,
            _precision(avg_batch_ms),
            avg_batch_ms,
            _precision(avg_pg_events_ms),
            avg_pg_events_ms,
            _precision(avg_valkey_ms),
            avg_valkey_ms,
            _precision(avg_pg_sessions_ms),
            avg_pg_sessions_ms,
        )

        # Log loop timing stats if available
        if self._period_loop_count > 0:
            avg_poll_ms = self._period_poll_ms / self._period_loop_count
            avg_commit_ms = self._period_commit_ms / self._period_loop_count
            avg_idle_ms = self._period_idle_ms / self._period_loop_count
            avg_fill = (
                sum(self._period_fill_ratios) / len(self._period_fill_ratios)
                if self._period_fill_ratios
                else 0.0
            )
            # Headroom: idle as percentage of total loop cycle
            avg_loop_ms = avg_poll_ms + avg_commit_ms + avg_idle_ms
            headroom = (avg_idle_ms / avg_loop_ms * 100) if avg_loop_ms > 0 else 0
            self._log.info(
                "Loop (%.1fs): avg_poll=%.*fms avg_commit=%.*fms "
                "avg_idle=%.*fms (headroom: %d%%) | "
                "fill=%d%% | poll_proximity=%.2f",
                elapsed,
                _precision(avg_poll_ms),
                avg_poll_ms,
                _precision(avg_commit_ms),
                avg_commit_ms,
                _precision(avg_idle_ms),
                avg_idle_ms,
                int(headroom),
                int(avg_fill * 100),
                self._period_max_proximity,
            )

        # Check for frequent rebalances (last 5 minutes)
        recent_rebalances = sum(1 for t in self._rebalance_events if now - t <= 300)
        if recent_rebalances > 2:
            self._log.warning(
                "%d rebalances in last 5 minutes — consumer group unstable",
                recent_rebalances,
            )

        # Log cumulative totals
        total_elapsed = now - self._start_time
        overall_eps = self._total_events / total_elapsed if total_elapsed > 0 else 0
        self._log.info(
            "Cumulative: %s events in %d batches over %.1fs (%s events/sec)",
            f"{self._total_events:,}",
            self._total_batches,
            total_elapsed,
            f"{overall_eps:,.0f}",
        )

        # Invoke caller's summary callback (e.g., for consumer lag logging)
        if self._on_summary:
            try:
                self._on_summary()
            except Exception as e:
                self._log.debug("on_summary callback error: %s", e)

        # Reset period counters
        self._period_events = 0
        self._period_sessions = 0
        self._period_batches = 0
        self._period_pg_events_ms = 0.0
        self._period_valkey_ms = 0.0
        self._period_pg_sessions_ms = 0.0
        self._period_total_ms = 0.0
        self._period_poll_ms = 0.0
        self._period_commit_ms = 0.0
        self._period_loop_count = 0
        self._period_fill_ratios = []
        self._period_max_proximity = 0.0
        self._period_idle_ms = 0.0
        self._last_summary_time = now

    def log_final_summary(self) -> None:
        """
        Log final summary on shutdown.

        Should be called from each consumer's finally block.
        """
        total_elapsed = time.monotonic() - self._start_time
        if self._total_batches == 0:
            self._log.info("Final: no batches processed (%.1fs elapsed)", total_elapsed)
            return

        overall_eps = self._total_events / total_elapsed if total_elapsed > 0 else 0
        avg_batch_ms = self._cum_total_ms / self._total_batches
        avg_pg_events_ms = self._cum_pg_events_ms / self._total_batches
        avg_valkey_ms = self._cum_valkey_ms / self._total_batches
        avg_pg_sessions_ms = self._cum_pg_sessions_ms / self._total_batches

        self._log.info(
            "Final: %s events, %s sessions in %d batches over %.1fs "
            "(%s events/sec) | "
            "avg_batch=%.*fms | "
            "avg_pg_events=%.*fms avg_valkey=%.*fms avg_pg_sessions=%.*fms",
            f"{self._total_events:,}",
            f"{self._total_sessions:,}",
            self._total_batches,
            total_elapsed,
            f"{overall_eps:,.0f}",
            _precision(avg_batch_ms),
            avg_batch_ms,
            _precision(avg_pg_events_ms),
            avg_pg_events_ms,
            _precision(avg_valkey_ms),
            avg_valkey_ms,
            _precision(avg_pg_sessions_ms),
            avg_pg_sessions_ms,
        )

        # Log final loop timing stats if available
        if self._cum_loop_count > 0:
            avg_poll_ms = self._cum_poll_ms / self._cum_loop_count
            avg_commit_ms = self._cum_commit_ms / self._cum_loop_count
            avg_idle_ms = self._cum_idle_ms / self._cum_loop_count
            avg_fill = self._cum_fill_sum / self._cum_loop_count
            avg_loop_ms = avg_poll_ms + avg_commit_ms + avg_idle_ms
            headroom = (avg_idle_ms / avg_loop_ms * 100) if avg_loop_ms > 0 else 0
            self._log.info(
                "Final loop: avg_poll=%.*fms avg_commit=%.*fms "
                "avg_idle=%.*fms (headroom: %d%%) | "
                "fill=%d%% | max_poll_proximity=%.2f | rebalances=%d",
                _precision(avg_poll_ms),
                avg_poll_ms,
                _precision(avg_commit_ms),
                avg_commit_ms,
                _precision(avg_idle_ms),
                avg_idle_ms,
                int(headroom),
                int(avg_fill * 100),
                self._cum_max_proximity,
                len(self._rebalance_events),
            )


def _precision(ms: float) -> int:
    """Return decimal precision for millisecond values.

    >= 10ms  → 0 decimals (e.g., 85ms)
    >= 1ms   → 1 decimal  (e.g., 3.2ms)
    < 1ms    → 2 decimals (e.g., 0.45ms)
    """
    if ms >= 10:
        return 0
    elif ms >= 1:
        return 1
    else:
        return 2
