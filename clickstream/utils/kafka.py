# ==============================================================================
# Kafka Utilities (Backward Compatibility)
# ==============================================================================
"""
Shared utilities for Kafka client configuration and message processing.

Note: This module re-exports from clickstream.infrastructure.kafka
for backward compatibility. New code should import directly from
clickstream.infrastructure.kafka.
"""

# Re-export from infrastructure.kafka for backward compatibility
from clickstream.infrastructure.kafka import (
    REQUIRED_EVENT_FIELDS,
    build_kafka_config,
    parse_kafka_messages,
)

__all__ = [
    "REQUIRED_EVENT_FIELDS",
    "build_kafka_config",
    "parse_kafka_messages",
]
