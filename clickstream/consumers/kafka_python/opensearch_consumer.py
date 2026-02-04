# ==============================================================================
# kafka-python OpenSearch Consumer
# ==============================================================================
"""
OpenSearch consumer implementation using kafka-python.

This is the baseline consumer implementation with no external streaming
framework dependencies. Uses:
- kafka-python for Kafka consumption with manual offset commits
- OpenSearchRepository for event indexing

Other frameworks can delegate to this consumer when they don't have
their own implementation.
"""

import json
import logging
import signal

from clickstream.infrastructure.kafka import build_kafka_config
from clickstream.infrastructure.search.opensearch import OpenSearchRepository
from clickstream.utils.config import get_settings
from clickstream.utils.session_state import set_last_message_timestamp

logger = logging.getLogger(__name__)

# Global flag for shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals by setting flag."""
    global _shutdown_requested
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown_requested = True


def run() -> None:
    """
    Run the OpenSearch consumer using kafka-python.

    This function:
    1. Creates a KafkaConsumer with manual offset commits
    2. Initializes OpenSearch repository
    3. Processes events in batches:
       - Indexes events to OpenSearch
       - Tracks activity timestamp for status display
       - Commits offsets
    """
    global _shutdown_requested
    _shutdown_requested = False

    # Set up signal handlers
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    from kafka import KafkaConsumer

    settings = get_settings()
    consumer_settings = settings.consumer
    group_id = settings.opensearch.consumer_group_id

    # Build Kafka consumer config
    kafka_config = build_kafka_config(settings.kafka)
    consumer = KafkaConsumer(
        settings.kafka.events_topic,
        **kafka_config,
        group_id=group_id,
        auto_offset_reset=consumer_settings.auto_offset_reset,
        enable_auto_commit=False,  # Manual commits after batch
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    # Initialize OpenSearch repository
    opensearch_repo = OpenSearchRepository(settings)
    opensearch_repo.connect()

    logger.info("Starting OpenSearch consumer (kafka-python)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", settings.kafka.events_topic)
    logger.info("Index: %s", settings.opensearch.events_index)

    try:
        while not _shutdown_requested:
            # Poll batch of messages
            raw_messages = consumer.poll(
                timeout_ms=consumer_settings.poll_timeout_ms,
                max_records=consumer_settings.batch_size,
            )

            if not raw_messages:
                continue

            # Flatten partition messages into event list
            events = []
            for partition_messages in raw_messages.values():
                for msg in partition_messages:
                    events.append(msg.value)

            if not events:
                continue

            try:
                # Index to OpenSearch
                opensearch_repo.save(events)

                # Track activity for status display
                set_last_message_timestamp(group_id)

                # Commit offsets
                consumer.commit()

                logger.debug("Indexed batch: %d events", len(events))

            except Exception as e:
                logger.error("Error processing batch: %s", e)
                # OpenSearch repository doesn't have rollback, but we can reconnect
                try:
                    opensearch_repo.close()
                    opensearch_repo.connect()
                    logger.info("Reconnected to OpenSearch after error")
                except Exception as reconnect_error:
                    logger.error("Failed to reconnect: %s", reconnect_error)
                    raise

    except Exception as e:
        logger.exception("Consumer error: %s", e)
        raise

    finally:
        consumer.close()
        opensearch_repo.close()
        logger.info("OpenSearch consumer shutdown complete.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
