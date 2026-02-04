# ==============================================================================
# Bytewax Kafka Configuration
# ==============================================================================
"""
Kafka configuration helpers for Bytewax dataflows.

Supports both local Docker (PLAINTEXT) and Aiven (SSL) deployments.
Bytewax uses confluent-kafka under the hood, which requires different
config key names than kafka-python.
"""

import logging
from typing import Dict, List, Optional

from clickstream.utils.config import get_settings
from clickstream.utils.paths import get_project_root

logger = logging.getLogger(__name__)


def get_kafka_brokers() -> List[str]:
    """
    Get Kafka broker list for Bytewax.

    Returns:
        List of broker addresses (Bytewax expects a list)
    """
    settings = get_settings()
    # Bytewax expects list of broker strings
    return settings.kafka.bootstrap_servers.split(",")


def get_kafka_add_config(
    group_id: Optional[str] = None,
    auto_commit: bool = True,
) -> Dict[str, str]:
    """
    Build additional Kafka config for Bytewax connectors.

    Bytewax uses confluent-kafka which requires different config key names
    than kafka-python. This function builds the add_config dict for
    kop.input() and KafkaSource().

    Note: Bytewax's KafkaSource sets group.id to "BYTEWAX_IGNORED" by default
    and manages offsets through its own recovery system. However, we override
    this with our own group.id and enable auto-commit so that Kafka consumer
    group offsets are tracked for monitoring purposes (e.g., lag calculation).

    Args:
        group_id: Consumer group ID (required for consumer)
        auto_commit: Enable auto-commit of offsets (default: True for monitoring)

    Returns:
        Dict with confluent-kafka compatible configuration
    """
    settings = get_settings()
    config: Dict[str, str] = {}

    # Consumer group configuration
    if group_id:
        config["group.id"] = group_id
        config["enable.auto.commit"] = str(auto_commit).lower()

    # SSL configuration for Aiven
    if settings.kafka.security_protocol == "SSL":
        project_root = get_project_root()
        config["security.protocol"] = "SSL"

        if settings.kafka.ssl_ca_file:
            ca_path = project_root / settings.kafka.ssl_ca_file
            if ca_path.exists():
                config["ssl.ca.location"] = str(ca_path)

        if settings.kafka.ssl_cert_file:
            cert_path = project_root / settings.kafka.ssl_cert_file
            if cert_path.exists():
                config["ssl.certificate.location"] = str(cert_path)

        if settings.kafka.ssl_key_file:
            key_path = project_root / settings.kafka.ssl_key_file
            if key_path.exists():
                config["ssl.key.location"] = str(key_path)

    return config


def ensure_topic_exists(
    topic_name: str,
    num_partitions: int,
    replication_factor: int = 1,
) -> None:
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

    from clickstream.infrastructure.kafka import build_kafka_config

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
