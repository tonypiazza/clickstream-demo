# ==============================================================================
# Confluent Kafka PostgreSQL Consumer
# ==============================================================================
"""
PostgreSQL consumer implementation using confluent-kafka (librdkafka C wrapper).

This is the highest-performance consumer option, using the native C library
for Kafka protocol handling. Key optimizations over kafka-python:

- Uses consume() batch API for efficient message retrieval
- Optimized fetch settings for higher throughput
- Pre-fetching with queued.min.messages for continuous processing
- Native C library handles protocol efficiently

Uses:
- confluent-kafka for Kafka consumption with manual offset commits
- PostgreSQLEventRepository for event persistence
- SessionState for Valkey-based session management
- PostgreSQLSessionRepository for session persistence
"""

import json
import logging
import signal
import time

from clickstream.consumers.batch_processor import BatchMetrics
from clickstream.utils.config import get_settings
from clickstream.utils.paths import get_project_root

logger = logging.getLogger(__name__)

# Global flag for shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals by setting flag."""
    global _shutdown_requested
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown_requested = True


def _build_consumer_config(
    kafka_settings, group_id: str, auto_offset_reset: str, benchmark_mode: bool = False
) -> dict:
    """
    Build confluent-kafka consumer configuration.

    Includes performance optimizations for high-throughput consumption.

    Args:
        kafka_settings: KafkaSettings instance
        group_id: Consumer group ID
        auto_offset_reset: Offset reset policy ('earliest', 'latest')
        benchmark_mode: If True, enable partition EOF reporting

    Returns:
        Dict with confluent-kafka consumer configuration
    """
    config = {
        "bootstrap.servers": kafka_settings.bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,  # Manual commit for reliability
        # Performance tuning - fetch settings
        "fetch.min.bytes": 1024,  # Wait for at least 1KB before returning
        "fetch.max.bytes": 52428800,  # 50MB max fetch size
        "max.partition.fetch.bytes": 1048576,  # 1MB per partition
        # Pre-fetching for continuous processing
        "queued.min.messages": 10000,  # Pre-fetch queue size
        "queued.max.messages.kbytes": 65536,  # 64MB pre-fetch buffer
        # Session management
        "session.timeout.ms": 45000,
        "heartbeat.interval.ms": 15000,
        # Reduce rebalance overhead
        "max.poll.interval.ms": 300000,  # 5 minutes max processing time
    }

    # Enable partition EOF reporting for benchmark mode
    if benchmark_mode:
        config["enable.partition.eof"] = True

    # Add SSL config if using SSL protocol
    if kafka_settings.security_protocol == "SSL":
        project_root = get_project_root()

        config["security.protocol"] = "SSL"

        if kafka_settings.ssl_ca_file:
            ca_path = project_root / kafka_settings.ssl_ca_file
            if ca_path.exists():
                config["ssl.ca.location"] = str(ca_path)

        if kafka_settings.ssl_cert_file:
            cert_path = project_root / kafka_settings.ssl_cert_file
            if cert_path.exists():
                config["ssl.certificate.location"] = str(cert_path)

        if kafka_settings.ssl_key_file:
            key_path = project_root / kafka_settings.ssl_key_file
            if key_path.exists():
                config["ssl.key.location"] = str(key_path)
    else:
        config["security.protocol"] = "PLAINTEXT"

    return config


def run() -> None:
    """
    Run the PostgreSQL consumer using confluent-kafka.

    This function:
    1. Creates a confluent-kafka Consumer with manual offset commits
    2. Initializes Valkey session state manager
    3. Processes events in batches:
       - Saves events to PostgreSQL
       - Updates sessions in Valkey (batch operation)
       - Saves sessions to PostgreSQL
       - Commits offsets

    In benchmark mode (CONSUMER_BENCHMARK_MODE=true), the consumer exits
    when all assigned partitions have reached EOF.

    Performance optimizations:
    - Uses consume() batch API instead of poll()
    - Optimized fetch settings for higher throughput
    - Pre-fetching for continuous processing
    """
    global _shutdown_requested
    _shutdown_requested = False

    # Set up signal handlers
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    from confluent_kafka import Consumer, KafkaError

    from clickstream.infrastructure.repositories.postgresql import (
        PostgreSQLEventRepository,
        PostgreSQLSessionRepository,
    )
    from clickstream.utils.session_state import (
        SessionState,
        get_valkey_client,
    )

    settings = get_settings()
    consumer_settings = settings.consumer
    group_id = settings.postgresql_consumer.group_id
    topic = settings.kafka.events_topic
    benchmark_mode = consumer_settings.benchmark_mode

    # Build confluent-kafka consumer config with optimizations
    kafka_config = _build_consumer_config(
        settings.kafka,
        group_id,
        consumer_settings.auto_offset_reset,
        benchmark_mode=benchmark_mode,
    )
    consumer = Consumer(kafka_config)

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
    def _log_consumer_lag():
        """Log consumer lag for all assigned partitions."""
        if not assigned_partitions:
            return
        from confluent_kafka import TopicPartition as _TP

        parts = []
        total_lag = 0
        for topic_name, part_idx in sorted(assigned_partitions):
            try:
                tp = _TP(topic_name, part_idx)
                _, high = consumer.get_watermark_offsets(tp, timeout=5.0)
                pos = consumer.position([tp])
                if pos and pos[0].offset >= 0:
                    lag = max(0, high - pos[0].offset)
                else:
                    lag = -1
            except Exception:
                lag = -1
            if lag >= 0:
                parts.append(f"p{part_idx}={lag:,}")
                total_lag += lag
            else:
                parts.append(f"p{part_idx}=?")
        logger.info("Consumer lag: %s | total=%s", " ".join(parts), f"{total_lag:,}")

    # Initialize batch metrics with lag callback
    batch_metrics = BatchMetrics(
        event_repo,
        session_state,
        session_repo,
        on_summary=_log_consumer_lag,
        log=logger,
    )

    # Track EOF partitions for benchmark mode
    eof_partitions: set[tuple[str, int]] = set()
    assigned_partitions: set[tuple[str, int]] = set()

    def _check_partitions_at_eof(consumer, partitions):
        """
        Check if any newly assigned partitions are already at EOF.

        After a rebalance, partitions that were previously at EOF won't re-report
        _PARTITION_EOF since there's no new data. We need to check their committed
        offset against the high watermark to detect this.

        Note: We use committed() instead of position() because position() returns
        an invalid offset immediately after assignment (before the consumer has
        seeked to the stored offset).
        """
        nonlocal eof_partitions
        for p in partitions:
            try:
                # Get high watermark (end offset) for this partition
                low, high = consumer.get_watermark_offsets(p, timeout=5.0)
                # Get committed offset (available immediately after assignment)
                committed = consumer.committed([p], timeout=5.0)
                if committed and committed[0].offset >= 0:
                    committed_offset = committed[0].offset
                    if committed_offset >= high:
                        # Already at EOF
                        eof_partitions.add((p.topic, p.partition))
                        logger.info(
                            "Partition %s-%d already at EOF (committed=%d, high=%d) (%d/%d assigned)",
                            p.topic,
                            p.partition,
                            committed_offset,
                            high,
                            len(eof_partitions),
                            len(assigned_partitions),
                        )
            except Exception as e:
                logger.debug("Could not check EOF for %s-%d: %s", p.topic, p.partition, e)

    # Callback to track assigned partitions
    def on_assign(consumer, partitions):
        nonlocal assigned_partitions
        assigned_partitions = {(p.topic, p.partition) for p in partitions}
        logger.info(
            "Assigned %d partitions: %s",
            len(partitions),
            [f"{p.topic}-{p.partition}" for p in partitions],
        )
        # In benchmark mode, check if any reassigned partitions are already at EOF
        if benchmark_mode and partitions:
            _check_partitions_at_eof(consumer, partitions)

    def on_revoke(consumer, partitions):
        nonlocal assigned_partitions, eof_partitions
        revoked = {(p.topic, p.partition) for p in partitions}
        assigned_partitions -= revoked
        eof_partitions -= revoked
        logger.info("Revoked %d partitions", len(partitions))

    # Subscribe with callbacks (must be defined before this call)
    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

    logger.info("Starting PostgreSQL consumer (confluent-kafka)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", topic)
    if benchmark_mode:
        logger.info("Benchmark mode: will exit when all assigned partitions reach EOF")

    try:
        while not _shutdown_requested:
            # Use consume() batch API - more efficient than poll()
            # Returns list of messages directly (not dict of partition -> messages)
            t_poll_start = time.monotonic()
            messages = consumer.consume(
                num_messages=10000,  # Larger batch for better throughput
                timeout=consumer_settings.poll_timeout_ms / 1000.0,  # Convert to seconds
            )
            poll_ms = (time.monotonic() - t_poll_start) * 1000

            if not messages:
                # In benchmark mode, check if all assigned partitions have reached EOF
                if benchmark_mode and assigned_partitions and eof_partitions >= assigned_partitions:
                    logger.info(
                        "All %d assigned partitions reached EOF, exiting benchmark mode",
                        len(assigned_partitions),
                    )
                    break
                continue

            # Process messages, filtering out errors
            events = []
            for msg in messages:
                if msg.error():
                    error = msg.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        # Track EOF partition for benchmark mode
                        eof_partitions.add((msg.topic(), msg.partition()))
                        logger.info(
                            "Partition %s-%d reached EOF (%d/%d assigned)",
                            msg.topic(),
                            msg.partition(),
                            len(eof_partitions),
                            len(assigned_partitions),
                        )
                        continue
                    else:
                        logger.error("Consumer error: %s", error)
                        continue

                # Deserialize JSON message
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    events.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Failed to decode message: %s", e)
                    continue

            # In benchmark mode, check if all assigned partitions have reached EOF
            if benchmark_mode and assigned_partitions and eof_partitions >= assigned_partitions:
                # Process any remaining events before exiting
                if events:
                    try:
                        batch_metrics.process_batch(events)
                        consumer.commit()
                    except Exception as e:
                        logger.error("Error processing final batch: %s", e)
                logger.info("All partitions reached EOF, exiting benchmark mode")
                break

            if not events:
                continue

            try:
                # Log poll timing when batch is non-empty
                logger.info("Poll: %.0fms (%s messages)", poll_ms, f"{len(messages):,}")

                # Process batch with instrumented 3-step pipeline
                batch_metrics.process_batch(events)

                # Commit offsets (timed)
                t_commit = time.monotonic()
                consumer.commit()
                commit_ms = (time.monotonic() - t_commit) * 1000
                logger.info("Commit: %.0fms", commit_ms)

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
