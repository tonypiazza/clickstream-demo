"""
PostgreSQL consumer using Quix Streams.

Consumes events from Kafka, updates session state in Valkey,
and writes events + sessions to PostgreSQL.
"""

import logging

from clickstream.framework.quix.config import create_application, create_topic, ensure_topic_exists
from clickstream.framework.quix.sinks.postgresql import (
    PostgreSQLEventSink,
    PostgreSQLSessionSink,
)
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
    3. Sets up event and session sinks with batching optimization

    The data flow is:
    - Events arrive from Kafka in batches
    - Events sink: writes raw events to PostgreSQL events table
    - Sessions sink: batches Valkey updates (2 round-trips per batch),
      then upserts resulting sessions to PostgreSQL

    Performance note: The sessions sink uses batch_update_sessions() instead of
    per-event update_session() calls. This is critical for remote Valkey servers
    (e.g., Aiven) where network latency would otherwise limit throughput to ~7 events/sec.
    """
    settings = get_settings()

    # Ensure topic exists before starting consumer (Quix throws if topic doesn't exist)
    ensure_topic_exists(
        settings.kafka.events_topic,
        settings.kafka.events_topic_partitions,
    )

    # Consumer group for PostgreSQL pipeline (separate from OpenSearch consumer)
    consumer_group = settings.postgresql_consumer.group_id

    # Create Quix application
    app = create_application(
        consumer_group=consumer_group,
        auto_offset_reset="earliest",
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

    # Initialize sinks
    # Events sink writes raw events to PostgreSQL
    events_sink = PostgreSQLEventSink(settings, group_id=consumer_group)
    # Sessions sink handles Valkey batching internally (2 round-trips per batch)
    # This is much faster than per-event updates when using remote Valkey (e.g., Aiven)
    sessions_sink = PostgreSQLSessionSink(settings, session_state=session_state)

    # Build streaming dataframe
    sdf = app.dataframe(topic)

    # Sink original events to PostgreSQL events table
    sdf.sink(events_sink)

    # Sink events to sessions sink - it handles Valkey batching + PostgreSQL upserts internally
    # This avoids the slow sdf.apply(process_event) pattern that made per-event Valkey calls
    sdf.sink(sessions_sink)

    logger.info("Starting PostgreSQL consumer (Quix Streams)...")
    logger.info("Consumer group: %s", consumer_group)
    logger.info("Topic: %s", settings.kafka.events_topic)

    app.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
