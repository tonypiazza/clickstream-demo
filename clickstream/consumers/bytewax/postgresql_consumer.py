# ==============================================================================
# PostgreSQL Consumer using Bytewax
# ==============================================================================
"""
PostgreSQL consumer using Bytewax dataflow.

Consumes events from Kafka, updates session state in Valkey,
and writes events + sessions to PostgreSQL.
"""

import json
import logging

from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main
from confluent_kafka import OFFSET_STORED

from clickstream.consumers.bytewax.config import (
    ensure_topic_exists,
    get_kafka_add_config,
    get_kafka_brokers,
)
from clickstream.consumers.bytewax.sinks.postgresql import PostgreSQLSink
from clickstream.utils.config import get_settings
from clickstream.utils.session_state import SessionState, get_valkey_client

logger = logging.getLogger(__name__)


def run():
    """
    Run the PostgreSQL consumer dataflow.

    This function:
    1. Creates a Bytewax Dataflow with Kafka input
    2. Initializes Valkey session state manager
    3. Sets up unified sink for events and sessions

    The data flow is:
    - Events arrive from Kafka
    - Unified sink: writes events to PostgreSQL, updates Valkey, upserts sessions
    """
    settings = get_settings()

    # Ensure topic exists before starting consumer
    ensure_topic_exists(
        settings.kafka.events_topic,
        settings.kafka.events_topic_partitions,
    )

    # Consumer group for PostgreSQL pipeline
    consumer_group = settings.postgresql_consumer.group_id
    brokers = get_kafka_brokers()
    # Enable auto-commit so Kafka consumer group offsets are tracked for monitoring
    add_config = get_kafka_add_config(group_id=consumer_group)

    # Initialize Valkey session state
    valkey_client = get_valkey_client()
    session_state = SessionState(
        valkey_client,
        timeout_minutes=settings.postgresql_consumer.session_timeout_minutes,
        ttl_hours=settings.valkey.session_ttl_hours,
    )

    # Create dataflow
    flow = Dataflow("postgresql_consumer")

    # Kafka input with consumer group
    # Using kop.input for Kafka operator-based input
    # batch_size=5000 improves throughput by fetching more messages per poll
    # starting_offset=OFFSET_STORED uses committed consumer group offsets
    kinp = kop.input(
        "kafka_in",
        flow,
        brokers=brokers,
        topics=[settings.kafka.events_topic],
        add_config=add_config,
        batch_size=5000,
        starting_offset=OFFSET_STORED,
    )

    # Extract event dict from Kafka message
    def extract_event(msg):
        """Extract and parse event from Kafka message."""
        return json.loads(msg.value.decode("utf-8"))

    # Process successful messages (kinp.oks filters out errors)
    events = op.map("extract", kinp.oks, extract_event)

    # Unified sink for events + sessions
    # Processes both in a single write_batch() call for optimal performance
    unified_sink = PostgreSQLSink(
        settings,
        session_state=session_state,
        group_id=consumer_group,
    )
    op.output("postgresql_out", events, unified_sink)

    logger.info("Starting PostgreSQL consumer (Bytewax)...")
    logger.info("Consumer group: %s", consumer_group)
    logger.info("Topic: %s", settings.kafka.events_topic)

    # Run dataflow with single worker
    cli_main(flow, workers_per_process=1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
