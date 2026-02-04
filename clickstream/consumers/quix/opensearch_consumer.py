"""
OpenSearch consumer using Quix Streams.

Consumes events from Kafka and indexes them to OpenSearch.
Uses a separate consumer group for independent backfill capability.
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
    """
    settings = get_settings()

    # Use OpenSearch-specific consumer group for independent offset tracking
    consumer_group = settings.opensearch.consumer_group_id

    # Ensure topic exists before starting consumer (Quix throws if topic doesn't exist)
    ensure_topic_exists(
        settings.kafka.events_topic,
        settings.kafka.events_topic_partitions,
    )

    # Create Quix application
    app = create_application(
        consumer_group=consumer_group,
        auto_offset_reset="earliest",
    )

    # Create topic
    topic = create_topic(app, settings.kafka.events_topic)

    # Initialize sink
    events_sink = OpenSearchEventSink(settings, group_id=consumer_group)

    # Build streaming dataframe
    sdf = app.dataframe(topic)

    # Sink events to OpenSearch
    sdf.sink(events_sink)

    logger.info("Starting OpenSearch consumer (Quix Streams)...")
    logger.info("Consumer group: %s", consumer_group)
    logger.info("Topic: %s", settings.kafka.events_topic)
    logger.info("Index: %s", settings.opensearch.events_index)

    app.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
