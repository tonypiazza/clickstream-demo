# ==============================================================================
# OpenSearch Consumer using Bytewax
# ==============================================================================
"""
OpenSearch consumer using Bytewax dataflow.

Consumes events from Kafka and indexes them to OpenSearch.
Uses a separate consumer group from the PostgreSQL consumer,
enabling independent processing and automatic backfill.
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
from clickstream.consumers.bytewax.sinks.opensearch import OpenSearchEventSink
from clickstream.utils.config import get_settings

logger = logging.getLogger(__name__)


def run():
    """
    Run the OpenSearch consumer dataflow.

    This function:
    1. Creates a Bytewax Dataflow with Kafka input
    2. Sets up OpenSearch event sink for indexing

    The data flow is:
    - Events arrive from Kafka (separate consumer group)
    - Events are bulk-indexed to OpenSearch
    """
    settings = get_settings()

    # Check if OpenSearch is enabled
    if not settings.opensearch.enabled:
        logger.warning("OpenSearch is disabled. Set OPENSEARCH_ENABLED=true to enable.")
        return

    # Ensure topic exists before starting consumer
    ensure_topic_exists(
        settings.kafka.events_topic,
        settings.kafka.events_topic_partitions,
    )

    # Consumer group for OpenSearch pipeline (separate from PostgreSQL)
    consumer_group = settings.opensearch.consumer_group_id
    brokers = get_kafka_brokers()
    add_config = get_kafka_add_config(group_id=consumer_group, auto_commit=True)

    # Create dataflow
    flow = Dataflow("opensearch_consumer")

    # Kafka input with consumer group
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

    # Process successful messages
    events = op.map("extract", kinp.oks, extract_event)

    # Sink to OpenSearch
    opensearch_sink = OpenSearchEventSink(settings, group_id=consumer_group)
    op.output("opensearch_out", events, opensearch_sink)

    logger.info("Starting OpenSearch consumer (Bytewax)...")
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
