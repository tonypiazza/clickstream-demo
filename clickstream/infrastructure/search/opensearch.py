# ==============================================================================
# OpenSearch Repository Implementation
# ==============================================================================
"""
OpenSearch implementation of the SearchRepository interface.

Provides:
- OpenSearchRepository: Bulk index events to OpenSearch
"""

import logging
from datetime import UTC, datetime

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

from clickstream.base.repositories import SearchRepository
from clickstream.utils.config import Settings, get_settings

logger = logging.getLogger(__name__)


class OpenSearchRepository(SearchRepository):
    """
    OpenSearch implementation of SearchRepository.

    Uses opensearch-py bulk helper for high-performance indexing.
    Documents are indexed with an explicit _id for deduplication based on
    visitor_id, timestamp, event type, and item_id.
    """

    def __init__(self, settings: Settings | None = None, index_name: str | None = None):
        """
        Initialize the OpenSearch repository.

        Args:
            settings: Application settings. If None, uses get_settings().
            index_name: Override index name. If None, uses settings.opensearch.events_index.
        """
        self._settings = settings or get_settings()
        self._client: OpenSearch | None = None
        self._index = index_name or self._settings.opensearch.events_index

    @property
    def index_name(self) -> str:
        """Get the index name."""
        return self._index

    @property
    def client(self) -> OpenSearch | None:
        """Get the OpenSearch client."""
        return self._client

    def connect(self) -> None:
        """Establish connection to OpenSearch."""
        os_settings = self._settings.opensearch

        self._client = OpenSearch(
            hosts=os_settings.hosts,
            http_auth=(os_settings.user, os_settings.password),
            use_ssl=os_settings.use_ssl,
            verify_certs=os_settings.verify_certs,
            ssl_show_warn=False,
            timeout=30,
            max_retries=3,
            retry_on_timeout=True,
        )

        # Verify connection
        info = self._client.info()
        logger.info(
            "OpenSearchRepository connected (cluster=%s, index=%s)",
            info.get("cluster_name", "unknown"),
            self._index,
        )

    def save(self, documents: list[dict]) -> int:
        """
        Index documents to OpenSearch.

        Args:
            documents: List of event dictionaries with keys:
                - timestamp: Unix timestamp in milliseconds
                - visitor_id: Visitor identifier
                - event: Event type ('view', 'addtocart', 'transaction')
                - item_id: Item identifier
                - transaction_id: Optional transaction identifier

        Returns:
            Count of documents successfully indexed
        """
        if self._client is None:
            raise RuntimeError("OpenSearch connection not established. Call connect() first.")

        if not documents:
            return 0

        # Transform documents to bulk format
        actions = []
        for doc in documents:
            # Convert timestamp (Unix ms) to ISO format
            event_time = datetime.fromtimestamp(doc["timestamp"] / 1000.0, tz=UTC)

            # Create document with explicit _id for deduplication
            doc_id = f"{doc['visitor_id']}_{doc['timestamp']}_{doc['event']}_{doc['item_id']}"

            actions.append(
                {
                    "_index": self._index,
                    "_id": doc_id,
                    "_source": {
                        "event_time": event_time.isoformat(),
                        "timestamp": doc["timestamp"],
                        "visitor_id": doc["visitor_id"],
                        "event": doc["event"],
                        "item_id": doc["item_id"],
                        "transaction_id": doc.get("transaction_id"),
                    },
                }
            )

        success, errors = bulk(
            self._client,
            actions,
            raise_on_error=False,
            raise_on_exception=False,
        )

        if errors:
            # Log first few errors for debugging
            error_count = len(errors) if isinstance(errors, list) else 1
            logger.warning("Bulk indexing had %d errors", error_count)
            if isinstance(errors, list) and errors:
                for err in errors[:3]:
                    logger.debug("Bulk error: %s", err)

        logger.debug("Indexed %d documents to OpenSearch", success)
        return success

    def save_with_errors(self, documents: list[dict]) -> tuple[int, list]:
        """
        Index documents to OpenSearch, returning detailed error info.

        Args:
            documents: List of event dictionaries

        Returns:
            Tuple of (success_count, errors_list)
        """
        if self._client is None:
            raise RuntimeError("OpenSearch connection not established. Call connect() first.")

        if not documents:
            return 0, []

        # Transform documents to bulk format
        actions = []
        for doc in documents:
            event_time = datetime.fromtimestamp(doc["timestamp"] / 1000.0, tz=UTC)
            doc_id = f"{doc['visitor_id']}_{doc['timestamp']}_{doc['event']}_{doc['item_id']}"

            actions.append(
                {
                    "_index": self._index,
                    "_id": doc_id,
                    "_source": {
                        "event_time": event_time.isoformat(),
                        "timestamp": doc["timestamp"],
                        "visitor_id": doc["visitor_id"],
                        "event": doc["event"],
                        "item_id": doc["item_id"],
                        "transaction_id": doc.get("transaction_id"),
                    },
                }
            )

        success, errors = bulk(
            self._client,
            actions,
            raise_on_error=False,
            raise_on_exception=False,
        )

        return success, errors if isinstance(errors, list) else []

    def close(self) -> None:
        """Close connection and release resources."""
        if self._client:
            try:
                self._client.close()
                logger.info("OpenSearchRepository connection closed")
            except Exception as e:
                logger.warning("Error closing connection: %s", e)
            finally:
                self._client = None


def check_opensearch_connection(settings: Settings | None = None) -> bool:
    """
    Check if OpenSearch is reachable.

    Args:
        settings: Application settings. If None, uses get_settings().

    Returns:
        True if connection successful, False otherwise
    """
    try:
        settings = settings or get_settings()
        os_settings = settings.opensearch

        client = OpenSearch(
            hosts=os_settings.hosts,
            http_auth=(os_settings.user, os_settings.password),
            use_ssl=os_settings.use_ssl,
            verify_certs=os_settings.verify_certs,
            ssl_show_warn=False,
            timeout=5,
        )
        client.info()
        client.close()
        return True
    except Exception:
        return False
