# ==============================================================================
# Infrastructure Adapters
# ==============================================================================
"""
Adapters for external services (ports-and-adapters architecture).

This module contains concrete implementations that any framework can use:
- cache/ - Cache adapters (Valkey/Redis)
- repositories/ - Database adapters (PostgreSQL, etc.)
- search/ - Search engine adapters (OpenSearch, etc.)
- kafka.py - Kafka configuration and admin operations
- metrics.py - Pipeline tracking (throughput, lag, timestamps)
- session_state.py - Session state persistence (Valkey)

Note: Process management (starting/stopping consumers/producers) is handled
in cli/shared.py as it's inherently framework-specific.
"""

from clickstream.infrastructure.cache import ValkeyCache, check_valkey_connection
from clickstream.infrastructure.kafka import (
    build_kafka_config,
    check_kafka_connection,
    get_admin_client,
    get_consumer_lag,
    get_topic_partition_count,
    parse_kafka_messages,
    purge_topic,
    reset_consumer_group,
)
from clickstream.infrastructure.metrics import (
    PipelineMetrics,
    get_last_message_timestamp,
    get_producer_messages,
    get_throughput_stats,
    increment_producer_messages,
    record_stats_sample,
    set_last_message_timestamp,
    set_producer_messages,
)
from clickstream.infrastructure.repositories import (
    PostgreSQLEventRepository,
    PostgreSQLSessionRepository,
    check_postgresql_connection,
)
from clickstream.infrastructure.search import (
    OpenSearchRepository,
    check_opensearch_connection,
)
from clickstream.infrastructure.session_state import (
    ValkeySessionStateStore,
    clear_session_state,
    get_sessions,
    save_sessions,
)

__all__ = [
    # Cache
    "ValkeyCache",
    "check_valkey_connection",
    # Kafka
    "build_kafka_config",
    "check_kafka_connection",
    "get_admin_client",
    "get_consumer_lag",
    "get_topic_partition_count",
    "parse_kafka_messages",
    "purge_topic",
    "reset_consumer_group",
    # Metrics
    "PipelineMetrics",
    "get_last_message_timestamp",
    "get_producer_messages",
    "get_throughput_stats",
    "increment_producer_messages",
    "record_stats_sample",
    "set_last_message_timestamp",
    "set_producer_messages",
    # Repositories
    "PostgreSQLEventRepository",
    "PostgreSQLSessionRepository",
    "check_postgresql_connection",
    # Search
    "OpenSearchRepository",
    "check_opensearch_connection",
    # Session State
    "ValkeySessionStateStore",
    "clear_session_state",
    "get_sessions",
    "save_sessions",
]
