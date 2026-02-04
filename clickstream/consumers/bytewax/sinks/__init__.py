# ==============================================================================
# Bytewax Sinks
# ==============================================================================
"""
Custom Bytewax sinks for data persistence.

Exports PostgreSQL and OpenSearch sinks for use with Bytewax dataflows.
"""

from clickstream.consumers.bytewax.sinks.postgresql import (
    PostgreSQLEventSink,
    PostgreSQLSessionSink,
    PostgreSQLSink,
)
from clickstream.consumers.bytewax.sinks.opensearch import OpenSearchEventSink

__all__ = [
    "PostgreSQLEventSink",
    "PostgreSQLSessionSink",
    "PostgreSQLSink",
    "OpenSearchEventSink",
]
