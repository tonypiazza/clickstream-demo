"""
Custom Quix Streams sinks for clickstream processing.

Provides BatchingSink implementations for:
- PostgreSQL (events and sessions tables)
- OpenSearch (events index)
"""

from clickstream.framework.quix.sinks.opensearch import OpenSearchEventSink, OpenSearchEventsSink
from clickstream.framework.quix.sinks.postgresql import (
    PostgreSQLEventSink,
    PostgreSQLEventsSink,
    PostgreSQLSessionSink,
    PostgreSQLSessionsSink,
)

__all__ = [
    # New singular names (preferred)
    "PostgreSQLEventSink",
    "PostgreSQLSessionSink",
    "OpenSearchEventSink",
    # Deprecated plural names (backward compatibility)
    "PostgreSQLEventsSink",
    "PostgreSQLSessionsSink",
    "OpenSearchEventsSink",
]
