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
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main
from confluent_kafka import OFFSET_STORED

from clickstream.consumers.bytewax.config import (
    ensure_topic_exists,
    get_kafka_add_config,
    get_kafka_brokers,
)
from clickstream.consumers.bytewax.sinks.opensearch import OpenSearchEventSink
from clickstream.consumers.bytewax.sources.kafka import KafkaSourceWithCommit
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

    In benchmark mode (CONSUMER_BENCHMARK_MODE=true), the consumer exits
    when all assigned partitions have been fully consumed (tail=False).
    """
    settings = get_settings()
    benchmark_mode = settings.consumer.benchmark_mode

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
    # Get Kafka config with group.id and SSL settings
    add_config = get_kafka_add_config(group_id=consumer_group)

    # Create dataflow
    flow = Dataflow("opensearch_consumer")

    # Kafka input with custom source that commits offsets explicitly
    # In benchmark mode, tail=False causes the source to exit at EOF
    source = KafkaSourceWithCommit(
        brokers=brokers,
        topics=[settings.kafka.events_topic],
        add_config=add_config,
        batch_size=5000,
        starting_offset=OFFSET_STORED,
        tail=not benchmark_mode,  # tail=False means exit at EOF
    )
    kinp = op.input("kafka_in", flow, source)

    # Extract event dict from Kafka message
    def extract_event(msg):
        """Extract and parse event from Kafka message."""
        return json.loads(msg.value.decode("utf-8"))

    events = op.map("extract", kinp, extract_event)

    # Sink to OpenSearch
    opensearch_sink = OpenSearchEventSink(settings)
    op.output("opensearch_out", events, opensearch_sink)

    logger.info("Starting OpenSearch consumer (Bytewax)...")
    logger.info("Consumer group: %s", consumer_group)
    logger.info("Topic: %s", settings.kafka.events_topic)
    if benchmark_mode:
        logger.info("Benchmark mode: will exit when all partitions are fully consumed")

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
