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

logger = logging.getLogger(__name__)

# Global flag for shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals by setting flag."""
    global _shutdown_requested
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown_requested = True


def _check_all_partitions_at_end(consumer) -> bool:
    """
    Check if all assigned partitions have been fully consumed.

    Compares current position to end offsets (high watermarks) for each
    partition assigned to this consumer instance.

    Args:
        consumer: KafkaConsumer instance

    Returns:
        True if all assigned partitions are at the end
    """
    assigned = consumer.assignment()
    if not assigned:
        return False

    # Get end offsets for all assigned partitions
    end_offsets = consumer.end_offsets(assigned)

    for tp in assigned:
        current_position = consumer.position(tp)
        end_offset = end_offsets.get(tp, 0)

        # If current position is less than end offset, not done yet
        if current_position < end_offset:
            return False

    return True


def run() -> None:
    """
    Run the OpenSearch consumer using kafka-python.

    This function:
    1. Creates a KafkaConsumer with manual offset commits
    2. Initializes OpenSearch repository
    3. Processes events in batches:
       - Indexes events to OpenSearch
       - Commits offsets

    In benchmark mode (CONSUMER_BENCHMARK_MODE=true), the consumer exits
    when all assigned partitions have been fully consumed.
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
    topic = settings.kafka.events_topic
    benchmark_mode = consumer_settings.benchmark_mode
    num_partitions = settings.kafka.events_topic_partitions

    # Build Kafka consumer config
    kafka_config = build_kafka_config(settings.kafka)
    consumer = KafkaConsumer(
        topic,
        **kafka_config,
        group_id=group_id,
        auto_offset_reset=consumer_settings.auto_offset_reset,
        enable_auto_commit=False,  # Manual commits after batch
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    # Initialize OpenSearch repository
    opensearch_repo = OpenSearchRepository(settings)
    opensearch_repo.connect()

    # Track consecutive empty polls for benchmark mode EOF detection
    empty_poll_count = 0
    max_empty_polls = 3  # Exit after 3 consecutive empty polls when at EOF

    logger.info("Starting OpenSearch consumer (kafka-python)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", topic)
    logger.info("Index: %s", settings.opensearch.events_index)
    if benchmark_mode:
        logger.info(
            "Benchmark mode: will exit when all %d partitions are fully consumed", num_partitions
        )

    try:
        while not _shutdown_requested:
            # Poll batch of messages
            raw_messages = consumer.poll(
                timeout_ms=consumer_settings.poll_timeout_ms,
                max_records=consumer_settings.batch_size,
            )

            if not raw_messages:
                # In benchmark mode, check if we've consumed everything
                if benchmark_mode:
                    empty_poll_count += 1
                    if empty_poll_count >= max_empty_polls:
                        if _check_all_partitions_at_end(consumer):
                            logger.info("All partitions fully consumed, exiting benchmark mode")
                            break
                        # Reset counter if not actually at end
                        empty_poll_count = 0
                continue

            # Reset empty poll counter on successful poll
            empty_poll_count = 0

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
