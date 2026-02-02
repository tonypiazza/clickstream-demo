# ==============================================================================
# Bytewax Sinks
# ==============================================================================
"""
Custom Bytewax sinks for data persistence.

Exports PostgreSQL and OpenSearch sinks for use with Bytewax dataflows.
"""

from clickstream.framework.bytewax.sinks.postgresql import (
    PostgreSQLEventSink,
    PostgreSQLSessionSink,
)
from clickstream.framework.bytewax.sinks.opensearch import OpenSearchEventSink

__all__ = [
    "PostgreSQLEventSink",
    "PostgreSQLSessionSink",
    "OpenSearchEventSink",
]
