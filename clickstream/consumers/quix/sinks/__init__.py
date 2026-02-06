"""
Custom Quix Streams sinks for clickstream processing.

Provides BatchingSink implementations for:
- PostgreSQL (events and sessions tables)
- OpenSearch (events index)
"""

from clickstream.consumers.quix.sinks.opensearch import OpenSearchEventSink
from clickstream.consumers.quix.sinks.postgresql import PostgreSQLSink

__all__ = [
    "OpenSearchEventSink",
    "PostgreSQLSink",
]
