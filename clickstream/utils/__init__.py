# ==============================================================================
# Clickstream Pipeline Utilities
# ==============================================================================
"""
Shared utilities for the clickstream pipeline.

This module exports configuration and models for use throughout the pipeline.
"""

from clickstream.core.models import (
    ClickstreamEvent,
    EventType,
    Session,
)
from clickstream.utils.config import (
    KafkaSettings,
    OpenSearchSettings,
    PostgreSQLConsumerSettings,
    PostgresSettings,
    ProducerSettings,
    Settings,
    get_settings,
)
from clickstream.utils.db import (
    ensure_schema,
    reset_schema,
)
from clickstream.utils.kafka import (
    REQUIRED_EVENT_FIELDS,
    parse_kafka_messages,
)

__all__ = [
    # Config
    "KafkaSettings",
    "OpenSearchSettings",
    "PostgresSettings",
    "PostgreSQLConsumerSettings",
    "ProducerSettings",
    "Settings",
    "get_settings",
    # Database
    "ensure_schema",
    "reset_schema",
    # Kafka utilities
    "REQUIRED_EVENT_FIELDS",
    "parse_kafka_messages",
    # Models
    "ClickstreamEvent",
    "EventType",
    "Session",
]
