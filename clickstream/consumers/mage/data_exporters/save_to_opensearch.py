"""
OpenSearch Data Exporter for Mage Pipeline.

This data exporter:
1. Indexes events to OpenSearch
2. Tracks last message timestamp for status display
3. Commits Kafka offsets after successful indexing
"""

from typing import Dict
import logging

if "data_exporter" not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

# Cache for reusable components (initialized lazily)
_opensearch_repo = None


def _get_repository():
    """Get or create OpenSearch repository instance."""
    global _opensearch_repo

    if _opensearch_repo is None:
        from clickstream.utils.config import get_settings
        from clickstream.infrastructure.search.opensearch import OpenSearchRepository

        settings = get_settings()
        _opensearch_repo = OpenSearchRepository(settings)
        _opensearch_repo.connect()

    return _opensearch_repo


@data_exporter
def save_to_opensearch(data: Dict, *args, **kwargs) -> Dict:
    """
    Index events to OpenSearch.

    Args:
        data: Dict with 'messages' (list of events) and 'consumer' from data loader

    Returns:
        Dict with processing status
    """
    from clickstream.utils.config import get_settings
    from clickstream.utils.session_state import set_last_message_timestamp

    events = data.get("messages", [])
    consumer = data.get("consumer")

    if not events:
        return {"status": "no_data", "events": 0}

    settings = get_settings()
    group_id = settings.opensearch.consumer_group_id
    opensearch_repo = _get_repository()

    try:
        # Index events to OpenSearch
        opensearch_repo.save(events)

        # Track activity for status display
        set_last_message_timestamp(group_id)

        # Commit Kafka offsets after successful indexing
        if consumer:
            consumer.commit()

        logger.debug("Indexed batch: %d events", len(events))

        return {
            "status": "success",
            "events": len(events),
        }

    except Exception as e:
        logger.error("Error indexing to OpenSearch: %s", e)

        # Attempt to reconnect
        try:
            opensearch_repo.close()
            opensearch_repo.connect()
            logger.info("Reconnected to OpenSearch after error")
        except Exception as reconnect_error:
            logger.error("Failed to reconnect: %s", reconnect_error)

        raise


@test
def test_output(output, *args) -> None:
    """Test that exporter returns valid output."""
    assert output is not None, "Output is undefined"
    assert "status" in output, "Output must contain status"
