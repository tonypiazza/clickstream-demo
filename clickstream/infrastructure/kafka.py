# ==============================================================================
# Kafka Infrastructure
# ==============================================================================
"""
Kafka client configuration, admin operations, and message processing utilities.

This module consolidates all Kafka-related functionality:
- Client configuration (SSL, PLAINTEXT, retries)
- Admin operations (topic management, consumer groups)
- Message parsing utilities
- Connection health checks

Supports both PLAINTEXT (local Docker) and SSL (Aiven mTLS) security protocols.
"""

import logging
import time
from typing import TYPE_CHECKING, Dict, Generator, List, Optional

from clickstream.utils.config import get_settings
from clickstream.utils.paths import get_project_root

if TYPE_CHECKING:
    from clickstream.utils.config import KafkaSettings

logger = logging.getLogger(__name__)


# ==============================================================================
# Configuration
# ==============================================================================

# Required fields for a valid clickstream event
REQUIRED_EVENT_FIELDS = ["timestamp", "visitor_id", "event", "item_id"]


def build_kafka_config(
    settings: "KafkaSettings | None" = None,
    include_serializers: bool = False,
    request_timeout_ms: int | None = None,
    include_retry_config: bool = True,
    include_producer_retries: bool = False,
) -> dict:
    """
    Build Kafka client configuration from settings.

    Supports PLAINTEXT (local) and SSL (Aiven mTLS) security protocols.
    Includes retry and reconnection parameters for network resilience.

    Args:
        settings: KafkaSettings instance. If None, loads from get_settings().
        include_serializers: If True, add JSON serializers (for producer)
        request_timeout_ms: Optional request timeout in milliseconds
        include_retry_config: If True, add connection retry/reconnect parameters (default: True)
        include_producer_retries: If True, add producer-specific retry settings (default: False)
            Note: 'retries' and 'retry_backoff_ms' are only valid for KafkaProducer,
            not for KafkaConsumer or KafkaAdminClient.

    Returns:
        Dict with Kafka client configuration
    """
    if settings is None:
        settings = get_settings().kafka

    config: dict = {
        "bootstrap_servers": settings.bootstrap_servers,
        "security_protocol": settings.security_protocol,
    }

    # Add connection retry/reconnect configuration for network resilience
    # These parameters are valid for all Kafka clients (producer, consumer, admin)
    if include_retry_config:
        config.update(
            {
                # Connection retry settings (applies to all clients)
                "reconnect_backoff_ms": 1000,
                "reconnect_backoff_max_ms": 32000,
                # Request timeout
                "request_timeout_ms": request_timeout_ms or 30000,
                # Keep connections alive longer to avoid unnecessary reconnects
                "connections_max_idle_ms": 540000,  # 9 minutes
            }
        )
    elif request_timeout_ms is not None:
        config["request_timeout_ms"] = request_timeout_ms

    # Add producer-specific retry settings
    # Note: These are ONLY valid for KafkaProducer, not Consumer or AdminClient
    if include_producer_retries:
        config.update(
            {
                "retries": 10,
                "retry_backoff_ms": 1000,
                # Batching for better throughput: wait up to 5ms to batch messages
                # This significantly improves producer throughput by reducing per-message overhead
                "linger_ms": 5,
                # Increase batch size for better throughput (default is 16KB)
                "batch_size": 65536,  # 64KB batches
            }
        )

    # Add SSL config if using SSL protocol
    if settings.security_protocol == "SSL":
        project_root = get_project_root()

        if settings.ssl_ca_file:
            ca_path = project_root / settings.ssl_ca_file
            if ca_path.exists():
                config["ssl_cafile"] = str(ca_path)

        if settings.ssl_cert_file:
            cert_path = project_root / settings.ssl_cert_file
            if cert_path.exists():
                config["ssl_certfile"] = str(cert_path)

        if settings.ssl_key_file:
            key_path = project_root / settings.ssl_key_file
            if key_path.exists():
                config["ssl_keyfile"] = str(key_path)

    # Add serializers for producer
    if include_serializers:
        import json

        config["value_serializer"] = lambda v: json.dumps(v).encode("utf-8")
        config["key_serializer"] = lambda k: str(k).encode("utf-8") if k is not None else None

    return config


# ==============================================================================
# Admin Client
# ==============================================================================


def get_admin_client(timeout_ms: int = 10000):
    """
    Get a Kafka admin client with current settings.

    Args:
        timeout_ms: Request timeout in milliseconds (default: 10000)

    Returns:
        KafkaAdminClient instance
    """
    from kafka import KafkaAdminClient

    config = build_kafka_config(request_timeout_ms=timeout_ms)
    return KafkaAdminClient(**config)


def check_kafka_connection() -> bool:
    """
    Check if Kafka is reachable.

    Returns:
        True if Kafka responds to list_topics(), False otherwise
    """
    try:
        admin_client = get_admin_client()
        admin_client.list_topics()
        admin_client.close()
        return True
    except Exception:
        return False


def list_topics() -> list[str]:
    """
    List all Kafka topics.

    Returns:
        List of topic names
    """
    admin = get_admin_client()
    try:
        topics = list(admin.list_topics())
        return topics
    finally:
        admin.close()


def get_topic_partition_count(topic_name: str) -> Optional[int]:
    """
    Get the number of partitions for a Kafka topic.

    Args:
        topic_name: Name of the topic

    Returns:
        Number of partitions, or None if topic doesn't exist
    """
    try:
        admin = get_admin_client()
        topics = admin.list_topics()
        if topic_name not in topics:
            admin.close()
            return None

        metadata = admin.describe_topics([topic_name])
        admin.close()
        if not metadata:
            return None

        return len(metadata[0].get("partitions", []))
    except Exception:
        return None


def purge_topic(topic_name: str) -> tuple[bool, int, str]:
    """
    Purge a Kafka topic by deleting and recreating it with the same config.

    Args:
        topic_name: Name of the topic to purge

    Returns:
        Tuple of (success, partition_count, error_message)
    """
    from kafka.admin import NewTopic
    from kafka.errors import UnknownTopicOrPartitionError

    try:
        admin = get_admin_client()

        # Check if topic exists and get its config
        topics = admin.list_topics()
        if topic_name not in topics:
            admin.close()
            return True, 0, "Topic does not exist"

        # Get topic metadata for partition count
        metadata = admin.describe_topics([topic_name])
        if not metadata:
            admin.close()
            return False, 0, "Could not get topic metadata"

        topic_metadata = metadata[0]
        num_partitions = len(topic_metadata.get("partitions", []))

        # Default replication factor if not found
        replication_factor = 1

        # Delete the topic
        try:
            admin.delete_topics([topic_name])
        except UnknownTopicOrPartitionError:
            pass  # Topic already deleted

        # Wait for deletion to complete
        time.sleep(2)

        # Recreate with same config
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions if num_partitions > 0 else 1,
            replication_factor=replication_factor,
        )
        admin.create_topics([new_topic])

        admin.close()
        return True, num_partitions, ""

    except Exception as e:
        return False, 0, str(e)


def reset_consumer_group(group_id: str) -> tuple[bool, str]:
    """
    Reset a Kafka consumer group by deleting it.

    Args:
        group_id: Consumer group ID to reset

    Returns:
        Tuple of (success, error_message)
    """
    try:
        admin = get_admin_client()

        # Delete the consumer group
        try:
            admin.delete_consumer_groups([group_id])
        except Exception:
            pass  # Group may not exist

        admin.close()
        return True, ""

    except Exception as e:
        return False, str(e)


# ==============================================================================
# Message Processing
# ==============================================================================


def parse_kafka_messages(
    messages: List[Dict],
) -> Generator[Dict, None, None]:
    """
    Parse Kafka messages and yield valid event dictionaries.

    Handles multiple message formats:
    - Direct event dict (from YAML-based Kafka sources)
    - Nested in 'data' key (some Kafka message wrappers)
    - Nested in 'value' key (Python @streaming_source classes)

    Skips messages missing required fields: timestamp, visitor_id, event, item_id.

    Args:
        messages: List of raw messages from Kafka

    Yields:
        Valid event dictionaries with required fields
    """
    for msg in messages:
        # Handle various message formats:
        # 1. Direct event dict (from YAML source)
        # 2. Nested in 'data' key (some Kafka wrappers)
        # 3. Nested in 'value' key (Python streaming source)
        if "value" in msg and isinstance(msg["value"], dict):
            event = msg["value"]
        elif "data" in msg and isinstance(msg["data"], dict):
            event = msg["data"]
        else:
            event = msg

        # Skip if missing required fields
        if not all(k in event for k in REQUIRED_EVENT_FIELDS):
            continue

        yield event
