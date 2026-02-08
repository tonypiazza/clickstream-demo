"""
PostgreSQL Data Exporter for Mage Pipeline.

This data exporter:
1. Saves events to PostgreSQL events table
2. Saves/updates sessions to PostgreSQL sessions table
3. Commits Kafka offsets after successful persistence
"""

import logging

if "data_exporter" not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

# Cache for reusable components (initialized lazily)
_event_repo = None
_session_repo = None


def _get_repositories():
    """Get or create repository instances."""
    global _event_repo, _session_repo

    if _event_repo is None or _session_repo is None:
        from clickstream.infrastructure.repositories.postgresql import (
            PostgreSQLEventRepository,
            PostgreSQLSessionRepository,
        )
        from clickstream.utils.config import get_settings

        settings = get_settings()

        _event_repo = PostgreSQLEventRepository(settings)
        _event_repo.connect()

        _session_repo = PostgreSQLSessionRepository(settings)
        _session_repo.connect()

    return _event_repo, _session_repo


@data_exporter
def save_to_postgresql(data: dict, *args, **kwargs) -> dict:
    """
    Save events and sessions to PostgreSQL.

    Args:
        data: Dict with 'events', 'sessions', and 'consumer' from transformer

    Returns:
        Dict with processing status
    """
    events = data.get("events", [])
    sessions = data.get("sessions", [])
    consumer = data.get("consumer")

    if not events:
        return {"status": "no_data", "events": 0, "sessions": 0}

    event_repo, session_repo = _get_repositories()

    try:
        # 1. Save events to PostgreSQL
        event_repo.save(events)

        # 2. Save sessions to PostgreSQL
        if sessions:
            session_repo.save(sessions)

        # 3. Commit Kafka offsets after successful persistence
        if consumer:
            consumer.commit()

        logger.debug(
            "Saved batch: %d events, %d sessions",
            len(events),
            len(sessions),
        )

        return {
            "status": "success",
            "events": len(events),
            "sessions": len(sessions),
        }

    except Exception as e:
        logger.error("Error saving to PostgreSQL: %s", e)

        # Rollback transactions
        try:
            event_repo.rollback()
            session_repo.rollback()
        except Exception:
            pass

        # Attempt to reconnect
        try:
            event_repo.reconnect()
            session_repo.reconnect()
            logger.info("Reconnected to PostgreSQL after error")
        except Exception as reconnect_error:
            logger.error("Failed to reconnect: %s", reconnect_error)

        raise


@test
def test_output(output, *args) -> None:
    """Test that exporter returns valid output."""
    assert output is not None, "Output is undefined"
    assert "status" in output, "Output must contain status"
