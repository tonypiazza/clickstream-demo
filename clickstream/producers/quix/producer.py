# ==============================================================================
# Quix Streams Producer
# ==============================================================================
"""
Quix Streams producer implementation.

Uses Quix Streams' producer API for publishing events to Kafka.

Performance optimization: Uses Polars for fast CSV loading instead of
row-by-row CSV reader with Pydantic validation.
"""

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


def run_producer(
    limit: Optional[int] = None,
    realtime: bool = False,
    speed: float = 1.0,
) -> None:
    """
    Run the Quix Streams producer.

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

    from clickstream.consumers.quix.config import create_application, ensure_topic_exists
    from clickstream.utils.config import get_settings
    from clickstream.utils.session_state import set_producer_messages

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

    # Ensure topic exists before producing
    ensure_topic_exists(
        settings.kafka.events_topic,
        settings.kafka.events_topic_partitions,
    )

    # Create Quix application (consumer_group not used for producer, but required)
    app = create_application(
        consumer_group="clickstream-producer",
        auto_offset_reset="earliest",
    )

    # Define topic with JSON serialization
    topic = app.topic(
        name=settings.kafka.events_topic,
        value_serializer="json",
        key_serializer="string",
    )

    try:
        events_sent = 0
        last_timestamp = None
        last_log_time = time.time()
        log_interval = 10  # Log progress every 10 seconds

        with app.get_producer() as producer:
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

                # Serialize and produce message
                # Use visitor_id as key for partition affinity (same visitor -> same partition)
                message = topic.serialize(key=str(event["visitor_id"]), value=event)
                producer.produce(
                    topic=topic.name,
                    key=message.key,
                    value=message.value,
                )
                events_sent += 1

                # Log progress periodically
                current_time = time.time()
                if current_time - last_log_time >= log_interval:
                    logger.info("Progress: %d events sent", events_sent)
                    last_log_time = current_time

        if _shutdown_requested:
            set_producer_messages(events_sent)
            logger.info("Producer interrupted by shutdown signal after %d events.", events_sent)
        else:
            set_producer_messages(events_sent)
            logger.info("Producer completed - %d events published.", events_sent)

    except Exception as e:
        logger.exception("Producer error: %s", e)
        sys.exit(1)

    logger.info("Producer shutdown complete.")


class QuixProducer(StreamingProducer):
    """
    Quix Streams producer implementation.

    Uses Quix Streams' producer API for publishing events to Kafka.
    """

    @property
    def name(self) -> str:
        return "quixstreams"

    @property
    def version(self) -> str:
        return f"quixstreams v{get_package_version('quixstreams')}"

    def run(
        self,
        limit: Optional[int] = None,
        realtime: bool = False,
        speed: float = 1.0,
    ) -> None:
        """Run the Quix producer."""
        run_producer(limit=limit, realtime=realtime, speed=speed)
