"""
Kafka Source for PostgreSQL Consumer Pipeline.

This data loader consumes events from Kafka for the PostgreSQL pipeline.
Uses the confluent-kafka library for high-performance message consumption.

Mage streaming pipelines expect:
- A generator or iterator that yields messages
- Each message is passed to downstream blocks for processing
"""

import json
import logging
from collections.abc import Generator

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)


def _build_consumer_config() -> dict:
    """Build Kafka consumer configuration from environment settings."""
    from clickstream.utils.config import get_settings
    from clickstream.utils.paths import get_project_root

    settings = get_settings()
    kafka_settings = settings.kafka
    consumer_settings = settings.consumer
    group_id = settings.postgresql_consumer.group_id

    config = {
        "bootstrap.servers": kafka_settings.bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": consumer_settings.auto_offset_reset,
        "enable.auto.commit": False,  # Manual commit for reliability
        # Performance tuning - fetch settings
        "fetch.min.bytes": 1024,
        "fetch.max.bytes": 52428800,
        "max.partition.fetch.bytes": 1048576,
        # Pre-fetching
        "queued.min.messages": 10000,
        "queued.max.messages.kbytes": 65536,
        # Session management
        "session.timeout.ms": 45000,
        "heartbeat.interval.ms": 15000,
        "max.poll.interval.ms": 300000,
    }

    # Add SSL config if using SSL protocol
    if kafka_settings.security_protocol == "SSL":
        project_root = get_project_root()
        config["security.protocol"] = "SSL"

        if kafka_settings.ssl_ca_file:
            ca_path = project_root / kafka_settings.ssl_ca_file
            if ca_path.exists():
                config["ssl.ca.location"] = str(ca_path)

        if kafka_settings.ssl_cert_file:
            cert_path = project_root / kafka_settings.ssl_cert_file
            if cert_path.exists():
                config["ssl.certificate.location"] = str(cert_path)

        if kafka_settings.ssl_key_file:
            key_path = project_root / kafka_settings.ssl_key_file
            if key_path.exists():
                config["ssl.key.location"] = str(key_path)
    else:
        config["security.protocol"] = "PLAINTEXT"

    return config


@data_loader
def load_from_kafka(*args, **kwargs) -> Generator[dict, None, None]:
    """
    Load streaming events from Kafka.

    This is a generator that continuously yields batches of events from Kafka.
    The generator will run indefinitely until the pipeline is stopped.

    Yields:
        Dict with 'messages' key containing list of event dicts,
        and 'consumer' key for offset commit after processing.
    """
    from confluent_kafka import Consumer, KafkaError

    from clickstream.utils.config import get_settings

    settings = get_settings()
    topic = settings.kafka.events_topic
    batch_size = settings.consumer.batch_size
    poll_timeout = settings.consumer.poll_timeout_ms / 1000.0

    # Build consumer config
    config = _build_consumer_config()
    consumer = Consumer(config)
    consumer.subscribe([topic])

    logger.info("Kafka source started for PostgreSQL consumer")
    logger.info("Consumer group: %s", config["group.id"])
    logger.info("Topic: %s", topic)

    try:
        while True:
            # Consume batch of messages
            messages = consumer.consume(
                num_messages=batch_size,
                timeout=poll_timeout,
            )

            if not messages:
                continue

            # Process messages, filtering out errors
            events = []
            for msg in messages:
                if msg.error():
                    error = msg.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error("Consumer error: %s", error)
                        continue

                # Deserialize JSON message
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    events.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Failed to decode message: %s", e)
                    continue

            if events:
                # Yield batch with consumer reference for commit
                yield {
                    "messages": events,
                    "consumer": consumer,
                }

    finally:
        consumer.close()
        logger.info("Kafka source shutdown complete")


@test
def test_output(output, *args) -> None:
    """Test that data loader returns valid output."""
    assert output is not None, "Output is undefined"
