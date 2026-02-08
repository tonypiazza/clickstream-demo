# ==============================================================================
# PostgreSQL Repository Implementations
# ==============================================================================
"""
PostgreSQL implementations of the repository interfaces.

Provides:
- PostgreSQLEventRepository: Bulk insert events with deduplication
- PostgreSQLSessionRepository: Upsert session aggregations
"""

import logging
from datetime import UTC, datetime

import psycopg2
from psycopg2.extras import execute_batch

from clickstream.base.repositories import EventRepository, SessionRepository
from clickstream.utils.config import Settings, get_settings

logger = logging.getLogger(__name__)

# Batch size for execute_batch
PAGE_SIZE = 1000

# Connection timeout
CONNECT_TIMEOUT = 10


def _add_connect_timeout(conn_string: str) -> str:
    """Add connect_timeout to connection string if not present."""
    if "connect_timeout" not in conn_string:
        separator = "&" if "?" in conn_string else "?"
        return f"{conn_string}{separator}connect_timeout={CONNECT_TIMEOUT}"
    return conn_string


class PostgreSQLEventRepository(EventRepository):
    """
    PostgreSQL implementation of EventRepository.

    Uses psycopg2.extras.execute_batch() for high-performance bulk inserts
    with ON CONFLICT DO NOTHING for deduplication.

    The events table has a unique constraint on (visitor_id, event_time, event, item_id)
    which ensures idempotent inserts.
    """

    def __init__(self, settings: Settings | None = None):
        """
        Initialize the event repository.

        Args:
            settings: Application settings. If None, uses get_settings().
        """
        self._settings = settings or get_settings()
        self._conn: psycopg2.extensions.connection | None = None
        self._schema = self._settings.postgres.schema_name

    @property
    def schema(self) -> str:
        """Get the database schema name."""
        return self._schema

    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        conn_string = _add_connect_timeout(self._settings.postgres.connection_string)
        self._conn = psycopg2.connect(conn_string)
        logger.info("PostgreSQLEventRepository connected (schema=%s)", self._schema)

    def save(self, events: list[dict]) -> int:
        """
        Persist events to PostgreSQL.

        Args:
            events: List of event dictionaries with keys:
                - timestamp: Unix timestamp in milliseconds
                - visitor_id: Visitor identifier
                - event: Event type ('view', 'addtocart', 'transaction')
                - item_id: Item identifier
                - transaction_id: Optional transaction identifier

        Returns:
            Count of events saved (may be less than input due to deduplication)
        """
        if self._conn is None:
            raise RuntimeError("PostgreSQL connection not established. Call connect() first.")

        if not events:
            return 0

        # Transform events to insert format
        messages = []
        for event in events:
            # Convert timestamp (Unix ms) to datetime
            event_time = datetime.fromtimestamp(event["timestamp"] / 1000.0, tz=UTC)
            messages.append(
                {
                    "event_time": event_time,
                    "visitor_id": event["visitor_id"],
                    "event": event["event"],
                    "item_id": event["item_id"],
                    "transaction_id": event.get("transaction_id"),
                }
            )

        with self._conn.cursor() as cur:
            execute_batch(
                cur,
                f"""
                INSERT INTO {self._schema}.events
                    (event_time, visitor_id, event, item_id, transaction_id)
                VALUES
                    (%(event_time)s, %(visitor_id)s, %(event)s::{self._schema}.event_type,
                     %(item_id)s, %(transaction_id)s)
                ON CONFLICT (visitor_id, event_time, event, item_id) DO NOTHING
                """,
                messages,
                page_size=PAGE_SIZE,
            )
        self._conn.commit()
        logger.debug("Inserted %d events", len(messages))
        return len(messages)

    def rollback(self) -> None:
        """Rollback current transaction."""
        if self._conn:
            try:
                self._conn.rollback()
            except Exception:
                pass

    def reconnect(self) -> None:
        """Attempt to reconnect to the database."""
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass

        conn_string = _add_connect_timeout(self._settings.postgres.connection_string)
        self._conn = psycopg2.connect(conn_string)
        logger.info("PostgreSQLEventRepository reconnected")

    def close(self) -> None:
        """Close connection and release resources."""
        if self._conn:
            try:
                self._conn.close()
                logger.info("PostgreSQLEventRepository connection closed")
            except Exception as e:
                logger.warning("Error closing connection: %s", e)
            finally:
                self._conn = None


class PostgreSQLSessionRepository(SessionRepository):
    """
    PostgreSQL implementation of SessionRepository.

    Uses psycopg2.extras.execute_batch() for high-performance upserts.
    Sessions are upserted using ON CONFLICT ... DO UPDATE to merge
    session data across multiple batches.
    """

    def __init__(self, settings: Settings | None = None):
        """
        Initialize the session repository.

        Args:
            settings: Application settings. If None, uses get_settings().
        """
        self._settings = settings or get_settings()
        self._conn: psycopg2.extensions.connection | None = None
        self._schema = self._settings.postgres.schema_name

    @property
    def schema(self) -> str:
        """Get the database schema name."""
        return self._schema

    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        conn_string = _add_connect_timeout(self._settings.postgres.connection_string)
        self._conn = psycopg2.connect(conn_string)
        logger.info("PostgreSQLSessionRepository connected (schema=%s)", self._schema)

    def save(self, sessions: list[dict]) -> int:
        """
        Persist sessions to PostgreSQL using upsert.

        Args:
            sessions: List of session dictionaries with keys:
                - session_id: Unique session identifier
                - visitor_id: Visitor identifier
                - session_start: Session start datetime
                - session_end: Session end datetime
                - duration_seconds: Session duration
                - event_count: Total events in session
                - view_count: View events count
                - cart_count: Add-to-cart events count
                - transaction_count: Transaction events count
                - items_viewed: List of viewed item IDs
                - items_carted: List of carted item IDs
                - items_purchased: List of purchased item IDs
                - converted: Boolean indicating conversion

        Returns:
            Count of sessions saved
        """
        if self._conn is None:
            raise RuntimeError("PostgreSQL connection not established. Call connect() first.")

        if not sessions:
            return 0

        with self._conn.cursor() as cur:
            execute_batch(
                cur,
                f"""
                INSERT INTO {self._schema}.sessions (
                    session_id, visitor_id, session_start, session_end,
                    duration_seconds, event_count, view_count, cart_count,
                    transaction_count, items_viewed, items_carted,
                    items_purchased, converted
                ) VALUES (
                    %(session_id)s, %(visitor_id)s, %(session_start)s, %(session_end)s,
                    %(duration_seconds)s, %(event_count)s, %(view_count)s, %(cart_count)s,
                    %(transaction_count)s, %(items_viewed)s, %(items_carted)s,
                    %(items_purchased)s, %(converted)s
                )
                ON CONFLICT (session_id) DO UPDATE SET
                    session_end = EXCLUDED.session_end,
                    duration_seconds = EXCLUDED.duration_seconds,
                    event_count = EXCLUDED.event_count,
                    view_count = EXCLUDED.view_count,
                    cart_count = EXCLUDED.cart_count,
                    transaction_count = EXCLUDED.transaction_count,
                    items_viewed = EXCLUDED.items_viewed,
                    items_carted = EXCLUDED.items_carted,
                    items_purchased = EXCLUDED.items_purchased,
                    converted = EXCLUDED.converted
                """,
                sessions,
                page_size=PAGE_SIZE,
            )
        self._conn.commit()
        logger.debug("Upserted %d sessions", len(sessions))
        return len(sessions)

    def rollback(self) -> None:
        """Rollback current transaction."""
        if self._conn:
            try:
                self._conn.rollback()
            except Exception:
                pass

    def reconnect(self) -> None:
        """Attempt to reconnect to the database."""
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass

        conn_string = _add_connect_timeout(self._settings.postgres.connection_string)
        self._conn = psycopg2.connect(conn_string)
        logger.info("PostgreSQLSessionRepository reconnected")

    def close(self) -> None:
        """Close connection and release resources."""
        if self._conn:
            try:
                self._conn.close()
                logger.info("PostgreSQLSessionRepository connection closed")
            except Exception as e:
                logger.warning("Error closing connection: %s", e)
            finally:
                self._conn = None


def check_postgresql_connection(settings: Settings | None = None) -> bool:
    """
    Check if PostgreSQL is reachable.

    Args:
        settings: Application settings. If None, uses get_settings().

    Returns:
        True if connection successful, False otherwise
    """
    try:
        settings = settings or get_settings()
        conn_string = _add_connect_timeout(settings.postgres.connection_string)
        conn = psycopg2.connect(conn_string)
        conn.close()
        return True
    except Exception:
        return False
