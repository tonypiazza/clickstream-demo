# ==============================================================================
# Bytewax Sinks
# ==============================================================================
"""
Custom Bytewax sinks for data persistence.

Exports PostgreSQL and OpenSearch sinks for use with Bytewax dataflows.
"""

from clickstream.consumers.bytewax.sinks.opensearch import OpenSearchEventSink
from clickstream.consumers.bytewax.sinks.postgresql import PostgreSQLSink

__all__ = [
    "OpenSearchEventSink",
    "PostgreSQLSink",
]
