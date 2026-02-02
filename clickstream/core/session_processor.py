# ==============================================================================
# Session Processor - Pure Domain Logic
# ==============================================================================
"""
Pure session processing logic with no external dependencies.

This module contains the domain logic for session aggregation:
- Session timeout detection
- Session creation and updates
- Event counting and item tracking
- Conversion to database record format

All methods work with plain dicts - no database, cache, or framework dependencies.
This allows the logic to be:
- Unit tested without mocks
- Reused across different streaming frameworks
- Composed with different persistence backends
"""

from datetime import datetime, timezone


class SessionProcessor:
    """
    Pure session processing logic.

    Handles session aggregation without any external dependencies.
    Works with plain dicts representing sessions and events.

    Session dict structure:
        {
            "session_id": str,           # "{visitor_id}_{session_num}"
            "visitor_id": int,
            "session_num": int,
            "session_start": int,        # Unix timestamp in milliseconds
            "session_end": int,          # Unix timestamp in milliseconds
            "event_count": int,
            "view_count": int,
            "cart_count": int,
            "transaction_count": int,
            "items_viewed": list[int],
            "items_carted": list[int],
            "items_purchased": list[int],
            "last_activity": int,        # Unix timestamp in milliseconds
        }

    Event dict structure:
        {
            "timestamp": int,            # Unix timestamp in milliseconds
            "visitor_id": int,
            "event": str,                # "view", "addtocart", "transaction"
            "item_id": int,
            "transaction_id": int | None,
        }
    """

    def __init__(self, timeout_minutes: int = 30):
        """
        Initialize session processor.

        Args:
            timeout_minutes: Session inactivity timeout in minutes.
                            A new session starts if the gap between events
                            exceeds this timeout.
        """
        self.timeout_ms = timeout_minutes * 60 * 1000

    def is_session_expired(self, session: dict | None, event_timestamp: int) -> bool:
        """
        Check if session has expired based on inactivity timeout.

        Args:
            session: Current session dict, or None if no session exists
            event_timestamp: Timestamp of the new event (ms since epoch)

        Returns:
            True if session is expired or doesn't exist
        """
        if session is None:
            return True
        last_activity = session.get("last_activity", 0)
        if last_activity == 0:
            return True
        gap = event_timestamp - last_activity
        return gap > self.timeout_ms

    def create_session(self, visitor_id: int, session_num: int, timestamp: int) -> dict:
        """
        Create a new empty session dict.

        Args:
            visitor_id: Visitor identifier
            session_num: Session number for this visitor (1-indexed)
            timestamp: Timestamp of the first event (ms since epoch)

        Returns:
            New session dict with initialized values
        """
        return {
            "session_id": f"{visitor_id}_{session_num}",
            "visitor_id": visitor_id,
            "session_num": session_num,
            "session_start": timestamp,
            "session_end": timestamp,
            "event_count": 0,
            "view_count": 0,
            "cart_count": 0,
            "transaction_count": 0,
            "items_viewed": [],
            "items_carted": [],
            "items_purchased": [],
            "last_activity": timestamp,
        }

    def update_session(self, session: dict, event: dict) -> dict:
        """
        Update session with event data.

        Mutates the session dict in place and returns it.

        Args:
            session: Session dict to update
            event: Event dict with timestamp, visitor_id, event, item_id

        Returns:
            The updated session dict (same object)
        """
        timestamp = event["timestamp"]
        event_type = event["event"]
        item_id = event["item_id"]

        # Update counters and timestamps
        session["event_count"] += 1
        session["session_end"] = max(session["session_end"], timestamp)
        session["last_activity"] = timestamp

        # Update event type counts and item lists
        if event_type == "view":
            session["view_count"] += 1
            if item_id not in session["items_viewed"]:
                session["items_viewed"].append(item_id)
        elif event_type == "addtocart":
            session["cart_count"] += 1
            if item_id not in session["items_carted"]:
                session["items_carted"].append(item_id)
        elif event_type == "transaction":
            session["transaction_count"] += 1
            if item_id not in session["items_purchased"]:
                session["items_purchased"].append(item_id)

        return session

    def process_events(
        self,
        events: list[dict],
        existing_sessions: dict[int, dict],
    ) -> dict[int, dict]:
        """
        Process multiple events against existing sessions.

        Events should be sorted by timestamp for correct session boundary detection.
        Mutates existing_sessions in place.

        Args:
            events: List of event dicts, should be sorted by timestamp
            existing_sessions: Dict mapping visitor_id to session dict.
                             Will be mutated with new/updated sessions.

        Returns:
            The updated sessions dict (same object as existing_sessions)
        """
        for event in events:
            visitor_id = event["visitor_id"]
            timestamp = event["timestamp"]

            current = existing_sessions.get(visitor_id)

            # Check if we need a new session
            if self.is_session_expired(current, timestamp):
                session_num = (current["session_num"] + 1) if current else 1
                current = self.create_session(visitor_id, session_num, timestamp)
                existing_sessions[visitor_id] = current

            # Update session with event
            self.update_session(current, event)

        return existing_sessions

    @staticmethod
    def to_db_record(session: dict) -> dict:
        """
        Convert session dict to database record format.

        Transforms timestamps from milliseconds to datetime objects
        and calculates derived fields.

        Args:
            session: Session dict from process_events() or update_session()

        Returns:
            Dict ready for database insert/upsert with:
            - session_id: str
            - visitor_id: int
            - session_start: datetime (UTC)
            - session_end: datetime (UTC)
            - duration_seconds: int
            - event_count: int
            - view_count: int
            - cart_count: int
            - transaction_count: int
            - items_viewed: list[int]
            - items_carted: list[int]
            - items_purchased: list[int]
            - converted: bool (True if transaction_count > 0)
        """
        session_start = datetime.fromtimestamp(session["session_start"] / 1000.0, tz=timezone.utc)
        session_end = datetime.fromtimestamp(session["session_end"] / 1000.0, tz=timezone.utc)
        duration_seconds = int((session_end - session_start).total_seconds())

        return {
            "session_id": session["session_id"],
            "visitor_id": session["visitor_id"],
            "session_start": session_start,
            "session_end": session_end,
            "duration_seconds": duration_seconds,
            "event_count": session["event_count"],
            "view_count": session["view_count"],
            "cart_count": session["cart_count"],
            "transaction_count": session["transaction_count"],
            "items_viewed": session["items_viewed"],
            "items_carted": session["items_carted"],
            "items_purchased": session["items_purchased"],
            "converted": session["transaction_count"] > 0,
        }
