"""
Event Processor Transformer for PostgreSQL Consumer.

This transformer:
1. Updates session state in Valkey for each event
2. Returns events and sessions for downstream export to PostgreSQL
"""

from typing import Dict
import logging

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

# Cache for reusable components (initialized lazily)
_session_state = None
_valkey_client = None


def _get_session_state():
    """Get or create SessionState instance."""
    global _session_state, _valkey_client

    if _session_state is None:
        from clickstream.utils.config import get_settings
        from clickstream.utils.session_state import SessionState, get_valkey_client

        settings = get_settings()
        _valkey_client = get_valkey_client()
        _session_state = SessionState(
            _valkey_client,
            timeout_minutes=settings.postgresql_consumer.session_timeout_minutes,
            ttl_hours=settings.valkey.session_ttl_hours,
        )

    return _session_state


@transformer
def process_events(data: Dict, *args, **kwargs) -> Dict:
    """
    Process events and update session state.

    Args:
        data: Dict with 'messages' (list of events) and 'consumer' (Kafka consumer)

    Returns:
        Dict with 'events', 'sessions', and 'consumer' for downstream processing
    """
    from clickstream.utils.config import get_settings

    events = data.get("messages", [])
    consumer = data.get("consumer")

    if not events:
        return {
            "events": [],
            "sessions": [],
            "consumer": consumer,
        }

    session_state = _get_session_state()

    try:
        # Batch update sessions in Valkey (efficient: 2 round-trips for batch)
        updated_sessions = session_state.batch_update_sessions(events)

        # Convert sessions to database record format
        session_records = [session_state.to_db_record(s) for s in updated_sessions]

        logger.debug(
            "Processed batch: %d events, %d sessions",
            len(events),
            len(session_records),
        )

        return {
            "events": events,
            "sessions": session_records,
            "consumer": consumer,
        }

    except Exception as e:
        logger.error("Error processing events: %s", e)
        raise


@test
def test_output(output, *args) -> None:
    """Test that transformer returns valid output."""
    assert output is not None, "Output is undefined"
    assert "events" in output, "Output must contain events"
    assert "sessions" in output, "Output must contain sessions"
