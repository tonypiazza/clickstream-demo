"""
Quix Streams Application factory with Kafka configuration.

Supports both local Docker (PLAINTEXT) and Aiven (SSL) deployments.
"""

import logging

from typing import Literal

from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

from clickstream.utils.config import get_settings

logger = logging.getLogger(__name__)

# Type alias for auto_offset_reset
AutoOffsetReset = Literal["earliest", "latest", "error"]


def ensure_topic_exists(topic_name: str, num_partitions: int, replication_factor: int = 1) -> None:
    """
    Ensure a Kafka topic exists, creating it if necessary.

    Args:
        topic_name: Name of the topic to ensure exists
        num_partitions: Number of partitions for the topic (if creating)
        replication_factor: Replication factor for the topic (if creating)
    """
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    from clickstream.utils.kafka import build_kafka_config

    settings = get_settings()
    config = build_kafka_config(settings.kafka)
    admin = KafkaAdminClient(**config)

    try:
        topics = admin.list_topics()
        if topic_name not in topics:
            logger.info("Creating topic '%s' with %d partitions", topic_name, num_partitions)
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            try:
                admin.create_topics([new_topic])
                logger.info("Topic '%s' created successfully", topic_name)
            except TopicAlreadyExistsError:
                # Race condition - another process created it
                logger.debug("Topic '%s' already exists (created by another process)", topic_name)
        else:
            logger.debug("Topic '%s' already exists", topic_name)
    finally:
        admin.close()


def create_application(
    consumer_group: str,
    auto_offset_reset: AutoOffsetReset = "earliest",
    benchmark_mode: bool = False,
    num_partitions: int = 3,
) -> Application:
    """
    Create a Quix Streams Application with proper Kafka configuration.

    Args:
        consumer_group: Kafka consumer group ID
        auto_offset_reset: Where to start reading if no committed offset
        benchmark_mode: If True, exit when all partitions reach EOF
        num_partitions: Number of topic partitions (for EOF tracking)

    Returns:
        Configured Quix Application instance
    """
    settings = get_settings()

    # Check if SSL is configured (Aiven uses mTLS)
    if settings.kafka.security_protocol == "SSL" and settings.kafka.ssl_ca_file:
        # Aiven: SSL with client certificates (mTLS)
        # Quix expects lowercase security_protocol
        connection = ConnectionConfig(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            security_protocol="ssl",
            ssl_ca_location=settings.kafka.ssl_ca_file,
            ssl_certificate_location=settings.kafka.ssl_cert_file,
            ssl_key_location=settings.kafka.ssl_key_file,
        )
    else:
        # Local Docker: PLAINTEXT (no auth)
        connection = ConnectionConfig(
            bootstrap_servers=settings.kafka.bootstrap_servers,
        )

    return Application(
        broker_address=connection,
        consumer_group=consumer_group,
        auto_offset_reset=auto_offset_reset,
        # Commit checkpoints every 1 second OR every 10000 messages (whichever is sooner)
        # Larger batch size improves throughput at the cost of less frequent status updates
        commit_interval=1.0,
        commit_every=10000,
        # Match poll timeout of other consumers (default is 1.0)
        consumer_poll_timeout=0.25,
        # Match librdkafka optimizations from confluent/mage consumers
        consumer_extra_config={
            "fetch.min.bytes": 1024,  # Wait for at least 1KB before returning
            "fetch.max.bytes": 52428800,  # 50MB max fetch size
            "max.partition.fetch.bytes": 1048576,  # 1MB per partition
            "queued.min.messages": 10000,  # Pre-fetch queue size
            "queued.max.messages.kbytes": 65536,  # 64MB pre-fetch buffer
        },
        # Disable changelog topics - using Valkey for state
        use_changelog_topics=False,
        # Don't auto-create topics (we manage via CLI)
        auto_create_topics=False,
    )


def create_topic(app: Application, topic_name: str):
    """
    Create a topic configuration for the application.

    Args:
        app: Quix Application instance
        topic_name: Name of the Kafka topic

    Returns:
        Topic instance for use with app.dataframe()
    """
    return app.topic(
        name=topic_name,
        value_deserializer="json",
        key_deserializer="bytes",
    )
