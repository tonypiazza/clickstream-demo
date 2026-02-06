"""
PostgreSQL consumer using Quix Streams.

Consumes events from Kafka, updates session state in Valkey,
and writes events + sessions to PostgreSQL.

In benchmark mode (CONSUMER_BENCHMARK_MODE=true), the consumer uses
app.run(timeout=1.0) to automatically exit 1 second after the last
message is consumed, enabling accurate throughput measurements.
"""

import logging

from clickstream.consumers.quix.config import create_application, create_topic, ensure_topic_exists
from clickstream.consumers.quix.sinks.postgresql import PostgreSQLSink
from clickstream.utils.config import get_settings
from clickstream.utils.session_state import (
    SessionState,
    get_valkey_client,
)

logger = logging.getLogger(__name__)


def run():
    """
    Run the PostgreSQL consumer.

    This function:
    1. Creates a Quix Application with Kafka configuration
    2. Initializes Valkey session state manager
    3. Sets up unified sink for events and sessions

    The data flow is:
    - Events arrive from Kafka in batches
    - Unified sink handles both events and sessions in a single write() call:
      1. Saves events to PostgreSQL
      2. Batch updates sessions in Valkey (2 round-trips per batch)
      3. Upserts sessions to PostgreSQL

    This single-sink pattern ensures reliable checkpoint/offset commits.

    In benchmark mode, uses app.run(timeout=1.0) to exit after 1 second
    of no new messages, enabling accurate throughput measurements.
    """
    settings = get_settings()
    benchmark_mode = settings.consumer.benchmark_mode
    num_partitions = settings.kafka.events_topic_partitions

    # Ensure topic exists before starting consumer (Quix throws if topic doesn't exist)
    ensure_topic_exists(
        settings.kafka.events_topic,
        num_partitions,
    )

    # Consumer group for PostgreSQL pipeline (separate from OpenSearch consumer)
    consumer_group = settings.postgresql_consumer.group_id

    # Create Quix application
    app = create_application(
        consumer_group=consumer_group,
        auto_offset_reset="earliest",
        benchmark_mode=benchmark_mode,
        num_partitions=num_partitions,
    )

    # Create topic
    topic = create_topic(app, settings.kafka.events_topic)

    # Initialize Valkey session state
    valkey_client = get_valkey_client()
    session_state = SessionState(
        valkey_client,
        timeout_minutes=settings.postgresql_consumer.session_timeout_minutes,
        ttl_hours=settings.valkey.session_ttl_hours,
    )

    # Initialize unified sink for events and sessions
    # Inject app's internal consumer for lag monitoring (private but stable attr)
    sink = PostgreSQLSink(
        settings=settings,
        session_state=session_state,
        consumer=getattr(app, "_consumer", None),
    )

    # Build streaming dataframe
    sdf = app.dataframe(topic)

    # Single sink handles both events and sessions in one write() call
    sdf.sink(sink)

    logger.info("Starting PostgreSQL consumer (Quix Streams)...")
    logger.info("Consumer group: %s", consumer_group)
    logger.info("Topic: %s", settings.kafka.events_topic)

    if benchmark_mode:
        logger.info("Benchmark mode: will exit 1 second after last message")
        app.run(timeout=1.0)
    else:
        app.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
