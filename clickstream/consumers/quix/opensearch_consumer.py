"""
OpenSearch consumer using Quix Streams.

Consumes events from Kafka and indexes them to OpenSearch.
Uses a separate consumer group for independent backfill capability.

In benchmark mode (CONSUMER_BENCHMARK_MODE=true), the consumer uses
app.run(timeout=1.0) to automatically exit 1 second after the last
message is consumed, enabling accurate throughput measurements.
"""

import logging

from clickstream.consumers.quix.config import create_application, create_topic, ensure_topic_exists
from clickstream.consumers.quix.sinks.opensearch import OpenSearchEventSink
from clickstream.utils.config import get_settings

logger = logging.getLogger(__name__)


def run():
    """
    Run the OpenSearch consumer.

    This function:
    1. Creates a Quix Application with separate consumer group
    2. Sets up OpenSearch sink
    3. Processes events and indexes to OpenSearch

    In benchmark mode, uses app.run(timeout=1.0) to exit after 1 second
    of no new messages, enabling accurate throughput measurements.
    """
    settings = get_settings()
    benchmark_mode = settings.consumer.benchmark_mode
    num_partitions = settings.kafka.events_topic_partitions

    # Use OpenSearch-specific consumer group for independent offset tracking
    consumer_group = settings.opensearch.consumer_group_id

    # Ensure topic exists before starting consumer (Quix throws if topic doesn't exist)
    ensure_topic_exists(
        settings.kafka.events_topic,
        num_partitions,
    )

    # Create Quix application
    app = create_application(
        consumer_group=consumer_group,
        auto_offset_reset="earliest",
        benchmark_mode=benchmark_mode,
        num_partitions=num_partitions,
    )

    # Create topic
    topic = create_topic(app, settings.kafka.events_topic)

    # Initialize sink
    events_sink = OpenSearchEventSink(settings)

    # Build streaming dataframe
    sdf = app.dataframe(topic)

    # Sink events to OpenSearch
    sdf.sink(events_sink)

    logger.info("Starting OpenSearch consumer (Quix Streams)...")
    logger.info("Consumer group: %s", consumer_group)
    logger.info("Topic: %s", settings.kafka.events_topic)
    logger.info("Index: %s", settings.opensearch.events_index)

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
