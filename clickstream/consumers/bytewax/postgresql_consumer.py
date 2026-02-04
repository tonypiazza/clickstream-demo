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
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main
from confluent_kafka import OFFSET_STORED

from clickstream.consumers.bytewax.config import (
    ensure_topic_exists,
    get_kafka_add_config,
    get_kafka_brokers,
)
from clickstream.consumers.bytewax.sinks.postgresql import PostgreSQLSink
from clickstream.consumers.bytewax.sources.kafka import KafkaSourceWithCommit
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
    # Get Kafka config with group.id and SSL settings
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

    # Kafka input with custom source that commits offsets explicitly
    # This ensures accurate lag tracking for monitoring and benchmarks
    source = KafkaSourceWithCommit(
        brokers=brokers,
        topics=[settings.kafka.events_topic],
        add_config=add_config,
        batch_size=5000,
        starting_offset=OFFSET_STORED,
    )
    kinp = op.input("kafka_in", flow, source)

    # Extract event dict from Kafka message
    def extract_event(msg):
        """Extract and parse event from Kafka message."""
        return json.loads(msg.value.decode("utf-8"))

    events = op.map("extract", kinp, extract_event)

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

    # Run dataflow with workers matching partition count
    # Bytewax distributes partitions round-robin across workers:
    # - Partition 0 -> Worker 0
    # - Partition 1 -> Worker 1
    # - Partition 2 -> Worker 2
    # This ensures each partition has exactly one consumer, preventing
    # offset commit conflicts when running a single Bytewax instance.
    num_workers = settings.kafka.events_topic_partitions
    logger.info("Workers per process: %d", num_workers)
    cli_main(flow, workers_per_process=num_workers)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
