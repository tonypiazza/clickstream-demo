# ==============================================================================
# Confluent Kafka Producer
# ==============================================================================
"""
Producer implementation using confluent-kafka (librdkafka C wrapper).

This is the highest-performance producer option, using the native C library
for Kafka protocol handling. Significantly faster than pure-Python kafka-python.

Performance optimization: Uses Polars for fast CSV loading.
"""

import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Generator, Optional

from clickstream.producers.base import StreamingProducer
from clickstream.utils.paths import get_project_root
from clickstream.utils.versions import get_package_version

logger = logging.getLogger(__name__)

# Global flag for shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals by setting flag."""
    global _shutdown_requested
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown_requested = True


def _get_events_file() -> Path:
    """Get path to the events CSV file."""
    # Check environment variable first
    events_file = os.environ.get("CLICKSTREAM_EVENTS_FILE")
    if events_file:
        return Path(events_file)

    # Default: data/events.csv relative to project root
    return get_project_root() / "data" / "events.csv"


def _read_events(
    filepath: Path, limit: int | None = None, offset: int = 0
) -> Generator[dict, None, None]:
    """
    Read events from CSV file using Polars for fast batch loading.

    Uses Polars DataFrame for efficient CSV parsing, which is significantly
    faster than row-by-row CSV reader with Pydantic validation.

    Args:
        filepath: Path to events CSV file
        limit: Maximum number of events to read (None for all)
        offset: Number of rows to skip before reading (0 = start at row 1)

    Yields:
        Event dictionaries ready for Kafka messages
    """
    import polars as pl

    # Load CSV with Polars (fast batch loading)
    df = pl.read_csv(filepath)

    # Rename columns to match our schema
    df = df.rename(
        {
            "visitorid": "visitor_id",
            "itemid": "item_id",
            "transactionid": "transaction_id",
        }
    )

    # Note: CSV is already sorted by timestamp, no need to sort again
    # (sorting 2.7M rows adds unnecessary O(n log n) overhead)

    # Apply offset using slice (much faster than skipping rows one-by-one)
    if offset > 0:
        df = df.slice(offset)

    # Apply limit
    if limit:
        df = df.head(limit)

    # Iterate over rows and yield event dictionaries
    for row in df.iter_rows(named=True):
        # Convert transaction_id to int if present (CSV reads it as string)
        tid = row.get("transaction_id")
        yield {
            "timestamp": row["timestamp"],
            "visitor_id": row["visitor_id"],
            "event": row["event"],
            "item_id": row["item_id"],
            "transaction_id": int(tid) if tid else None,
        }


def _build_confluent_config(settings) -> dict:
    """
    Build confluent-kafka configuration from settings.

    Note: confluent-kafka uses dot-notation keys (e.g., 'bootstrap.servers')
    unlike kafka-python which uses underscores (e.g., 'bootstrap_servers').

    Args:
        settings: KafkaSettings instance

    Returns:
        Dict with confluent-kafka producer configuration
    """
    config = {
        "bootstrap.servers": settings.bootstrap_servers,
        # Producer performance settings
        "linger.ms": 5,  # Wait up to 5ms to batch messages
        "batch.num.messages": 10000,  # Max messages per batch
        "queue.buffering.max.messages": 100000,  # Max messages in producer queue
        "queue.buffering.max.kbytes": 1048576,  # 1GB max buffer
        # Reliability settings
        "acks": "all",  # Wait for all replicas
        "retries": 10,
        "retry.backoff.ms": 100,
        # Compression for better throughput
        "compression.type": "lz4",
    }

    # Add SSL config if using SSL protocol
    if settings.security_protocol == "SSL":
        project_root = get_project_root()

        config["security.protocol"] = "SSL"

        if settings.ssl_ca_file:
            ca_path = project_root / settings.ssl_ca_file
            if ca_path.exists():
                config["ssl.ca.location"] = str(ca_path)

        if settings.ssl_cert_file:
            cert_path = project_root / settings.ssl_cert_file
            if cert_path.exists():
                config["ssl.certificate.location"] = str(cert_path)

        if settings.ssl_key_file:
            key_path = project_root / settings.ssl_key_file
            if key_path.exists():
                config["ssl.key.location"] = str(key_path)
    else:
        config["security.protocol"] = "PLAINTEXT"

    return config


def run_producer(
    limit: Optional[int] = None,
    realtime: bool = False,
    speed: float = 1.0,
) -> None:
    """
    Run the confluent-kafka producer.

    Args:
        limit: Maximum number of events to produce (None for all)
        realtime: Whether to replay events in real-time
        speed: Speed multiplier for real-time replay
    """
    global _shutdown_requested
    _shutdown_requested = False

    # Set up signal handlers
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    from confluent_kafka import Producer

    from clickstream.utils.config import get_settings
    from clickstream.utils.session_state import increment_producer_messages, set_producer_messages

    settings = get_settings()

    # Get offset from environment (used for resuming)
    offset_env = os.environ.get("PRODUCER_OFFSET")
    offset = int(offset_env) if offset_env else 0

    # Get events file path
    events_file = _get_events_file()
    if not events_file.exists():
        logger.error("Events file not found: %s", events_file)
        sys.exit(1)

    logger.info("Events file: %s", events_file)
    if realtime:
        logger.info("Mode: Real-time (%dx speed)", int(speed))
    else:
        logger.info("Mode: Batch (no delays)")
    if offset:
        logger.info("Offset: %d rows to skip", offset)
    if limit:
        logger.info("Limit: %d events", limit)
    else:
        logger.info("Limit: All events")

    # Build confluent-kafka configuration
    kafka_config = _build_confluent_config(settings.kafka)
    producer = Producer(kafka_config)

    topic = settings.kafka.events_topic

    # Reset counter at start of new producer run
    set_producer_messages(0)

    # Track delivery errors
    delivery_errors = 0

    def delivery_callback(err, msg):
        """Callback for message delivery reports."""
        nonlocal delivery_errors
        if err is not None:
            delivery_errors += 1
            if delivery_errors <= 10:  # Only log first 10 errors
                logger.error("Message delivery failed: %s", err)

    try:
        events_sent = 0
        last_increment = 0  # Track last increment point for Valkey updates
        last_timestamp = None
        last_log_time = time.time()
        log_interval = 10  # Log progress every 10 seconds
        increment_interval = 10000  # Update Valkey counter every 10,000 events
        poll_interval = 1000  # Poll for callbacks every 1000 messages

        for event in _read_events(events_file, limit, offset):
            if _shutdown_requested:
                logger.info("Shutdown requested, stopping producer...")
                break

            # In realtime mode, delay based on timestamp differences
            if realtime and last_timestamp is not None:
                # Calculate delay in seconds (timestamps are in milliseconds)
                delay_ms = (event["timestamp"] - last_timestamp) / speed
                if delay_ms > 0:
                    # Sleep in small increments to allow shutdown checks
                    delay_seconds = delay_ms / 1000.0
                    sleep_start = time.time()
                    while time.time() - sleep_start < delay_seconds:
                        if _shutdown_requested:
                            break
                        time.sleep(min(0.1, delay_seconds - (time.time() - sleep_start)))

            if _shutdown_requested:
                break

            last_timestamp = event["timestamp"]

            # Produce message with retry on queue full
            # Use visitor_id as key for partition affinity (same visitor -> same partition)
            max_retries = 5
            for retry in range(max_retries):
                try:
                    producer.produce(
                        topic,
                        key=str(event["visitor_id"]),
                        value=json.dumps(event),
                        callback=delivery_callback,
                    )
                    break  # Success
                except BufferError:
                    if retry == max_retries - 1:
                        raise  # Re-raise on final retry
                    # Queue full - poll to drain and wait before retry
                    producer.poll(1.0)  # Blocking poll for up to 1 second
                    time.sleep(0.1 * (retry + 1))  # Backoff: 0.1s, 0.2s, 0.3s, 0.4s
            events_sent += 1

            # Poll for delivery callbacks periodically (non-blocking)
            # This is important for confluent-kafka to process callbacks and free buffers
            if events_sent % poll_interval == 0:
                producer.poll(0)

            # Update Valkey counter every increment_interval events
            if events_sent - last_increment >= increment_interval:
                try:
                    increment_producer_messages(events_sent - last_increment)
                    last_increment = events_sent
                except Exception:
                    pass  # Don't fail producer if Valkey unavailable

            # Log progress periodically
            current_time = time.time()
            if current_time - last_log_time >= log_interval:
                logger.info("Progress: %d events sent", events_sent)
                last_log_time = current_time

        # Flush remaining messages (wait for all deliveries)
        logger.info("Flushing remaining messages...")
        producer.flush(timeout=30)

        # Update Valkey with any remaining events not yet counted
        remaining = events_sent - last_increment
        if remaining > 0:
            try:
                increment_producer_messages(remaining)
            except Exception:
                pass

        if delivery_errors > 0:
            logger.warning("Total delivery errors: %d", delivery_errors)

        if _shutdown_requested:
            logger.info("Producer interrupted by shutdown signal after %d events.", events_sent)
        else:
            logger.info("Producer completed - %d events published.", events_sent)

    except Exception as e:
        logger.exception("Producer error: %s", e)
        sys.exit(1)

    logger.info("Producer shutdown complete.")


class ConfluentProducer(StreamingProducer):
    """
    Producer implementation using confluent-kafka (librdkafka).

    This is the highest-performance producer option, using the native C library
    for Kafka protocol handling.
    """

    @property
    def name(self) -> str:
        return "confluent-kafka"

    @property
    def version(self) -> str:
        return f"confluent-kafka v{get_package_version('confluent-kafka')}"

    def run(
        self,
        limit: Optional[int] = None,
        realtime: bool = False,
        speed: float = 1.0,
    ) -> None:
        """Run the confluent-kafka producer."""
        run_producer(limit=limit, realtime=realtime, speed=speed)
