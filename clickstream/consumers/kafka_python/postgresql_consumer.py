# ==============================================================================
# kafka-python PostgreSQL Consumer
# ==============================================================================
"""
PostgreSQL consumer implementation using kafka-python.

This is the baseline consumer implementation with no external streaming
framework dependencies. Uses:
- kafka-python for Kafka consumption with manual offset commits
- PostgreSQLEventRepository for event persistence
- SessionState for Valkey-based session management
- PostgreSQLSessionRepository for session persistence

Other frameworks can delegate to this consumer when they don't have
their own implementation.
"""

import json
import logging
import os
import signal
import time

from clickstream.consumers.batch_processor import BatchMetrics
from clickstream.infrastructure.kafka import build_kafka_config
from clickstream.infrastructure.repositories.postgresql import (
    PostgreSQLEventRepository,
    PostgreSQLSessionRepository,
)
from clickstream.utils.config import get_settings
from clickstream.utils.session_state import (
    SessionState,
    get_valkey_client,
)

logger = logging.getLogger(__name__)

# Global flag for shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals by setting flag."""
    global _shutdown_requested
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown_requested = True


def _check_all_partitions_at_end(consumer) -> bool:
    """
    Check if all assigned partitions have been fully consumed.

    Compares current position to end offsets (high watermarks) for each
    partition assigned to this consumer instance.

    Args:
        consumer: KafkaConsumer instance

    Returns:
        True if all assigned partitions are at the end
    """
    assigned = consumer.assignment()
    if not assigned:
        return False

    # Get end offsets for all assigned partitions
    end_offsets = consumer.end_offsets(assigned)

    for tp in assigned:
        current_position = consumer.position(tp)
        end_offset = end_offsets.get(tp, 0)

        # If current position is less than end offset, not done yet
        if current_position < end_offset:
            return False

    return True


def run() -> None:
    """
    Run the PostgreSQL consumer using kafka-python.

    This function:
    1. Creates a KafkaConsumer with manual offset commits
    2. Initializes Valkey session state manager
    3. Processes events in batches:
       - Saves events to PostgreSQL
       - Updates sessions in Valkey (batch operation)
       - Saves sessions to PostgreSQL
       - Commits offsets

    In benchmark mode (CONSUMER_BENCHMARK_MODE=true), the consumer exits
    when all assigned partitions have been fully consumed.

    Performance note: Uses batch_update_sessions() for Valkey operations,
    which is critical for remote Valkey servers (e.g., Aiven) where network
    latency would otherwise limit throughput.
    """
    global _shutdown_requested
    _shutdown_requested = False

    # Set up signal handlers
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    from kafka import KafkaConsumer

    settings = get_settings()
    consumer_settings = settings.consumer
    group_id = settings.postgresql_consumer.group_id
    topic = settings.kafka.events_topic
    benchmark_mode = consumer_settings.benchmark_mode
    num_partitions = settings.kafka.events_topic_partitions

    # Build Kafka consumer config
    kafka_config = build_kafka_config(settings.kafka)
    consumer = KafkaConsumer(
        **kafka_config,
        group_id=group_id,
        auto_offset_reset=consumer_settings.auto_offset_reset,
        enable_auto_commit=False,  # Manual commits after batch
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        api_version=(2, 6, 0),  # Skip auto-detection for faster/more reliable startup
    )

    # Initialize repositories
    event_repo = PostgreSQLEventRepository(settings)
    event_repo.connect()

    session_repo = PostgreSQLSessionRepository(settings)
    session_repo.connect()

    # Initialize Valkey session state
    valkey_client = get_valkey_client()
    session_state = SessionState(
        valkey_client,
        timeout_minutes=settings.postgresql_consumer.session_timeout_minutes,
        ttl_hours=settings.valkey.session_ttl_hours,
    )

    # Consumer lag logging callback for periodic summaries
    instance = int(os.environ.get("CONSUMER_INSTANCE", "0"))

    def _log_consumer_lag():
        """Log consumer lag for all assigned partitions and persist to Valkey."""
        assigned = consumer.assignment()
        if not assigned:
            return
        end_offsets = consumer.end_offsets(assigned)
        parts = []
        total_lag = 0
        partition_lags: dict[int, int] = {}
        for tp in sorted(assigned, key=lambda tp: tp.partition):
            try:
                pos = consumer.position(tp)
                high = end_offsets.get(tp, 0)
                lag = max(0, high - pos)
            except Exception:
                lag = -1
            if lag >= 0:
                parts.append(f"p{tp.partition}={lag:,}")
                total_lag += lag
                partition_lags[tp.partition] = lag
            else:
                parts.append(f"p{tp.partition}=?")
        logger.info("Consumer lag: %s | total=%s", " ".join(parts), f"{total_lag:,}")

        # Persist lag sample to Valkey for time-series tracking
        if partition_lags:
            try:
                from clickstream.infrastructure.metrics import record_lag_sample

                record_lag_sample(group_id, instance, partition_lags)
            except Exception as e:
                logger.debug("Failed to record lag sample: %s", e)

        # Persist backpressure indicators to Valkey for CLI reporting
        try:
            from clickstream.infrastructure.metrics import record_backpressure_sample

            indicators = batch_metrics.get_backpressure_indicators()
            record_backpressure_sample(group_id, instance, indicators)
        except Exception as e:
            logger.debug("Failed to record backpressure sample: %s", e)

    # Initialize batch metrics with lag callback
    batch_metrics = BatchMetrics(
        event_repo,
        session_state,
        session_repo,
        on_summary=_log_consumer_lag,
        log=logger,
        max_poll_interval_ms=consumer_settings.max_poll_interval_ms,
    )

    # Rebalance listener for tracking partition assignment events
    from kafka.consumer.subscription_state import ConsumerRebalanceListener

    class RebalanceListener(ConsumerRebalanceListener):
        def on_partitions_revoked(self, revoked):
            logger.info("Revoked %d partitions", len(revoked))

        def on_partitions_assigned(self, assigned):
            logger.info(
                "Assigned %d partitions: %s",
                len(assigned),
                [f"{tp.topic}-{tp.partition}" for tp in assigned],
            )
            batch_metrics.record_rebalance()

    consumer.subscribe([topic], listener=RebalanceListener())

    # Track consecutive empty polls for benchmark mode EOF detection
    empty_poll_count = 0
    max_empty_polls = 3  # Exit after 3 consecutive empty polls when at EOF

    logger.info("Starting PostgreSQL consumer (kafka-python)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", topic)
    if benchmark_mode:
        logger.info(
            "Benchmark mode: will exit when all %d partitions are fully consumed", num_partitions
        )

    try:
        t_commit_end = 0.0  # Track commit end time for idle gap calculation
        while not _shutdown_requested:
            # Poll batch of messages
            t_poll_start = time.monotonic()
            idle_ms = (t_poll_start - t_commit_end) * 1000 if t_commit_end else 0.0
            raw_messages = consumer.poll(
                timeout_ms=consumer_settings.poll_timeout_ms,
                max_records=consumer_settings.batch_size,
            )
            poll_ms = (time.monotonic() - t_poll_start) * 1000

            if not raw_messages:
                # In benchmark mode, check if we've consumed everything
                if benchmark_mode:
                    empty_poll_count += 1
                    if empty_poll_count >= max_empty_polls:
                        if _check_all_partitions_at_end(consumer):
                            logger.info("All partitions fully consumed, exiting benchmark mode")
                            break
                        # Reset counter if not actually at end
                        empty_poll_count = 0
                continue

            # Reset empty poll counter on successful poll
            empty_poll_count = 0

            # Flatten partition messages into event list
            events = []
            for partition_messages in raw_messages.values():
                for msg in partition_messages:
                    events.append(msg.value)

            if not events:
                continue

            try:
                # Log poll timing when batch is non-empty
                msg_count = sum(len(msgs) for msgs in raw_messages.values())
                logger.info("Poll: %.0fms (%s messages)", poll_ms, f"{msg_count:,}")

                # Process batch with instrumented 3-step pipeline
                batch_metrics.process_batch(events)

                # Commit offsets (timed)
                t_commit = time.monotonic()
                consumer.commit()
                t_commit_end = time.monotonic()
                commit_ms = (t_commit_end - t_commit) * 1000
                logger.info("Commit: %.0fms", commit_ms)

                # Record loop timing for periodic summaries
                batch_metrics.record_loop_timing(
                    poll_ms=poll_ms,
                    commit_ms=commit_ms,
                    batch_size=msg_count,
                    max_batch_size=consumer_settings.batch_size,
                    idle_ms=idle_ms,
                )

            except Exception as e:
                logger.error("Error processing batch: %s", e)
                # Rollback any partial transactions
                event_repo.rollback()
                session_repo.rollback()

                # Attempt to reconnect on connection errors
                try:
                    event_repo.reconnect()
                    session_repo.reconnect()
                    logger.info("Reconnected to PostgreSQL after error")
                except Exception as reconnect_error:
                    logger.error("Failed to reconnect: %s", reconnect_error)
                    raise

    except Exception as e:
        logger.exception("Consumer error: %s", e)
        raise

    finally:
        batch_metrics.log_final_summary()
        consumer.close()
        event_repo.close()
        session_repo.close()
        logger.info("PostgreSQL consumer shutdown complete.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
