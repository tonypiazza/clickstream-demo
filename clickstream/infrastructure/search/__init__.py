# ==============================================================================
# Search Engine Adapters
# ==============================================================================
"""
Search engine adapters implementing the SearchRepository interface from base/repositories.py.

Currently supported:
- OpenSearch (opensearch.py)

Future:
- Elasticsearch, etc.
"""

from clickstream.infrastructure.search.opensearch import (
    OpenSearchRepository,
    check_opensearch_connection,
)

__all__ = [
    "OpenSearchRepository",
    "check_opensearch_connection",
]
