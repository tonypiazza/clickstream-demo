# ==============================================================================
# Mage Pipeline Executor
# ==============================================================================
"""
Mage pipeline executor.

Provides execute_streaming_pipeline() to run Mage streaming pipelines
as custom streaming loops that invoke the pipeline blocks directly.

Note: Mage's built-in StreamingPipelineExecutor requires specific source
connectors (Kafka, Kinesis, etc.) configured via YAML. To support SSL
and advanced Kafka configurations from environment variables, we implement
a custom streaming loop that invokes block functions directly.
"""

import json
import logging
import signal
import time
from typing import Any

logger = logging.getLogger(__name__)

# Global flag for shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals by setting flag."""
    global _shutdown_requested
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown_requested = True


def get_mage_project_path() -> str:
    """Get the path to the Mage project directory."""
    from clickstream.utils.paths import get_project_root

    return str(get_project_root() / "clickstream" / "consumers" / "mage")


def _build_consumer_config(group_id: str, benchmark_mode: bool = False) -> dict:
    """
    Build confluent-kafka consumer configuration.

    Args:
        group_id: Consumer group ID
        benchmark_mode: If True, enable partition EOF detection

    Returns:
        Dict with confluent-kafka consumer configuration
    """
    from clickstream.utils.config import get_settings
    from clickstream.utils.paths import get_project_root

    settings = get_settings()
    kafka_settings = settings.kafka
    consumer_settings = settings.consumer

    config = {
        "bootstrap.servers": kafka_settings.bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": consumer_settings.auto_offset_reset,
        "enable.auto.commit": False,
        "fetch.min.bytes": 1024,
        "fetch.max.bytes": 52428800,
        "max.partition.fetch.bytes": 1048576,
        "queued.min.messages": 10000,
        "queued.max.messages.kbytes": 65536,
        "session.timeout.ms": 45000,
        "heartbeat.interval.ms": 15000,
        "max.poll.interval.ms": 300000,
    }

    # Enable partition EOF detection for benchmark mode
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


def execute_streaming_pipeline(
    pipeline_name: str,
    variables: dict[str, Any] | None = None,
) -> None:
    """
    Execute a Mage streaming pipeline using custom streaming loop.

    This implementation uses confluent-kafka directly for message consumption
    and invokes Mage transformer/exporter blocks for processing. This approach
    supports SSL authentication and advanced Kafka configurations.

    Args:
        pipeline_name: Name of the streaming pipeline (e.g., "postgresql_consumer")
        variables: Optional pipeline variables

    Raises:
        ImportError: If required packages are not installed
        RuntimeError: If pipeline not found or execution fails
    """
    global _shutdown_requested
    _shutdown_requested = False

    # Set up signal handlers
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    from clickstream.utils.config import get_settings

    settings = get_settings()

    logger.info("Pipeline: %s", pipeline_name)

    if pipeline_name == "postgresql_consumer":
        _run_postgresql_consumer(settings)
    elif pipeline_name == "opensearch_consumer":
        _run_opensearch_consumer(settings)
    else:
        raise RuntimeError(f"Unknown pipeline: {pipeline_name}")


def _run_postgresql_consumer(settings) -> None:
    """Run the PostgreSQL consumer pipeline."""
    global _shutdown_requested

    from confluent_kafka import Consumer, KafkaError, TopicPartition

    from clickstream.consumers.batch_processor import BatchMetrics
    from clickstream.infrastructure.repositories.postgresql import (
        PostgreSQLEventRepository,
        PostgreSQLSessionRepository,
    )
    from clickstream.utils.session_state import (
        SessionState,
        get_valkey_client,
    )

    group_id = settings.postgresql_consumer.group_id
    topic = settings.kafka.events_topic
    batch_size = 10000  # Larger batch for better throughput
    poll_timeout = settings.consumer.poll_timeout_ms / 1000.0
    benchmark_mode = settings.consumer.benchmark_mode

    # Build consumer config
    config = _build_consumer_config(group_id, benchmark_mode=benchmark_mode)
    consumer = Consumer(config)

    # Track EOF and assigned partitions for benchmark mode
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

    def _check_all_partitions_at_end(consumer) -> bool:
        """
        Check if all assigned partitions have been fully consumed.

        This is a fallback for when EOF signals are missed after rebalance.
        Compares committed offsets against high watermarks.

        Returns:
            True if all partitions are at end, False otherwise
        """
        if not assigned_partitions:
            return False

        for topic_name, part_idx in assigned_partitions:
            try:
                tp = TopicPartition(topic_name, part_idx)
                low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
                committed = consumer.committed([tp], timeout=5.0)
                if committed and committed[0].offset >= 0:
                    if committed[0].offset < high:
                        return False  # This partition is not at end
                else:
                    return False  # No committed offset, not at end
            except Exception as e:
                logger.debug("Could not check watermarks for %s-%d: %s", topic_name, part_idx, e)
                return False
        return True

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

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
        parts = []
        total_lag = 0
        for topic_name, part_idx in sorted(assigned_partitions):
            try:
                tp = TopicPartition(topic_name, part_idx)
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

    logger.info("Starting PostgreSQL consumer (Mage AI)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", topic)
    if benchmark_mode:
        logger.info("Benchmark mode: will exit when all assigned partitions reach EOF")

    # Track consecutive empty polls for fallback EOF detection
    empty_poll_count = 0
    max_empty_polls = 5  # Exit after ~1.25 seconds of no messages (5 * 0.25s)

    try:
        while not _shutdown_requested:
            # Consume messages
            t_poll_start = time.monotonic()
            messages = consumer.consume(
                num_messages=batch_size,
                timeout=poll_timeout,
            )
            poll_ms = (time.monotonic() - t_poll_start) * 1000

            if not messages:
                # Check if all assigned partitions have reached EOF
                if benchmark_mode and assigned_partitions and eof_partitions >= assigned_partitions:
                    logger.info(
                        "All %d assigned partitions reached EOF, exiting benchmark mode",
                        len(assigned_partitions),
                    )
                    break

                # Fallback: after consecutive empty polls, check watermarks directly
                # This handles the case where EOF signals are missed after rebalance
                if benchmark_mode and assigned_partitions:
                    empty_poll_count += 1
                    if empty_poll_count >= max_empty_polls:
                        if _check_all_partitions_at_end(consumer):
                            logger.info(
                                "All %d assigned partitions at end (watermark check), exiting benchmark mode",
                                len(assigned_partitions),
                            )
                            break
                        # Reset counter to avoid spamming watermark checks
                        empty_poll_count = 0
                continue

            # Reset empty poll counter when we get messages
            empty_poll_count = 0

            # Parse messages
            events = []
            for msg in messages:
                if msg.error():
                    error = msg.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        # Track EOF partition
                        if benchmark_mode:
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

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    events.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Failed to decode message: %s", e)
                    continue

            if not events:
                # Check if all assigned partitions have reached EOF after processing messages
                if benchmark_mode and assigned_partitions and eof_partitions >= assigned_partitions:
                    logger.info(
                        "All %d assigned partitions reached EOF, exiting benchmark mode",
                        len(assigned_partitions),
                    )
                    break
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
                event_repo.rollback()
                session_repo.rollback()

                try:
                    event_repo.reconnect()
                    session_repo.reconnect()
                    logger.info("Reconnected to PostgreSQL after error")
                except Exception as reconnect_error:
                    logger.error("Failed to reconnect: %s", reconnect_error)
                    raise

    finally:
        batch_metrics.log_final_summary()
        consumer.close()
        event_repo.close()
        session_repo.close()
        logger.info("PostgreSQL consumer shutdown complete.")


def _run_opensearch_consumer(settings) -> None:
    """Run the OpenSearch consumer pipeline."""
    global _shutdown_requested

    from confluent_kafka import Consumer, KafkaError, TopicPartition

    from clickstream.infrastructure.search.opensearch import OpenSearchRepository

    group_id = settings.opensearch.consumer_group_id
    topic = settings.kafka.events_topic
    batch_size = 10000  # Larger batch for better throughput
    poll_timeout = settings.consumer.poll_timeout_ms / 1000.0
    benchmark_mode = settings.consumer.benchmark_mode

    # Build consumer config
    config = _build_consumer_config(group_id, benchmark_mode=benchmark_mode)
    consumer = Consumer(config)

    # Track EOF and assigned partitions for benchmark mode
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

    def _check_all_partitions_at_end(consumer) -> bool:
        """
        Check if all assigned partitions have been fully consumed.

        This is a fallback for when EOF signals are missed after rebalance.
        Compares committed offsets against high watermarks.

        Returns:
            True if all partitions are at end, False otherwise
        """
        if not assigned_partitions:
            return False

        for topic_name, part_idx in assigned_partitions:
            try:
                tp = TopicPartition(topic_name, part_idx)
                low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
                committed = consumer.committed([tp], timeout=5.0)
                if committed and committed[0].offset >= 0:
                    if committed[0].offset < high:
                        return False  # This partition is not at end
                else:
                    return False  # No committed offset, not at end
            except Exception as e:
                logger.debug("Could not check watermarks for %s-%d: %s", topic_name, part_idx, e)
                return False
        return True

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)

    # Initialize OpenSearch repository
    opensearch_repo = OpenSearchRepository(settings)
    opensearch_repo.connect()

    logger.info("Starting OpenSearch consumer (Mage AI)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", topic)
    logger.info("Index: %s", settings.opensearch.events_index)
    if benchmark_mode:
        logger.info("Benchmark mode: will exit when all assigned partitions reach EOF")

    # Track consecutive empty polls for fallback EOF detection
    empty_poll_count = 0
    max_empty_polls = 5  # Exit after ~1.25 seconds of no messages (5 * 0.25s)

    try:
        while not _shutdown_requested:
            # Consume messages
            messages = consumer.consume(
                num_messages=batch_size,
                timeout=poll_timeout,
            )

            if not messages:
                # Check if all assigned partitions have reached EOF
                if benchmark_mode and assigned_partitions and eof_partitions >= assigned_partitions:
                    logger.info(
                        "All %d assigned partitions reached EOF, exiting benchmark mode",
                        len(assigned_partitions),
                    )
                    break

                # Fallback: after consecutive empty polls, check watermarks directly
                # This handles the case where EOF signals are missed after rebalance
                if benchmark_mode and assigned_partitions:
                    empty_poll_count += 1
                    if empty_poll_count >= max_empty_polls:
                        if _check_all_partitions_at_end(consumer):
                            logger.info(
                                "All %d assigned partitions at end (watermark check), exiting benchmark mode",
                                len(assigned_partitions),
                            )
                            break
                        # Reset counter to avoid spamming watermark checks
                        empty_poll_count = 0
                continue

            # Reset empty poll counter when we get messages
            empty_poll_count = 0

            # Parse messages
            events = []
            for msg in messages:
                if msg.error():
                    error = msg.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        # Track EOF partition
                        if benchmark_mode:
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

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    events.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Failed to decode message: %s", e)
                    continue

            if not events:
                # Check if all assigned partitions have reached EOF after processing messages
                if benchmark_mode and assigned_partitions and eof_partitions >= assigned_partitions:
                    logger.info(
                        "All %d assigned partitions reached EOF, exiting benchmark mode",
                        len(assigned_partitions),
                    )
                    break
                continue

            try:
                # Index to OpenSearch
                opensearch_repo.save(events)

                # Commit offsets
                consumer.commit()

                logger.debug("Indexed batch: %d events", len(events))

            except Exception as e:
                logger.error("Error processing batch: %s", e)

                try:
                    opensearch_repo.close()
                    opensearch_repo.connect()
                    logger.info("Reconnected to OpenSearch after error")
                except Exception as reconnect_error:
                    logger.error("Failed to reconnect: %s", reconnect_error)
                    raise

    finally:
        consumer.close()
        opensearch_repo.close()
        logger.info("OpenSearch consumer shutdown complete.")
