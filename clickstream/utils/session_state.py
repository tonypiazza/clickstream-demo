# ==============================================================================
# Valkey (Redis) Utilities for Session State
# ==============================================================================
"""
Valkey/Redis utilities for managing session state in streaming pipelines.

This module provides:
- Session state storage with automatic TTL
- Atomic session updates using Redis transactions
- Retry logic with exponential backoff for network resilience

Note: Event deduplication is handled by PostgreSQL via a unique index
on (visitor_id, event_time, event, item_id) with ON CONFLICT DO NOTHING.

Session State Schema (per visitor):
- session:{visitor_id}:meta -> Hash with session metadata
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from clickstream.utils.config import get_settings
from clickstream.utils.retry import VALKEY_RETRIES

logger = logging.getLogger(__name__)


def get_valkey_client() -> redis.Redis:
    """
    Get a Valkey/Redis client connection.

    Configured with:
    - 10 second socket timeouts for fast failure detection
    - 10 automatic retries with exponential backoff for transient failures
    - Health check interval to keep connections alive

    Returns:
        redis.Redis client instance

    Note:
        For new code, prefer using ValkeyCache from infrastructure.cache
        which provides a higher-level interface with JSON serialization.
    """
    settings = get_settings()

    # Configure retry with exponential backoff for transient failures
    # 10 retries over ~60 seconds for network resilience
    retry = Retry(ExponentialBackoff(cap=32, base=1), retries=VALKEY_RETRIES)

    return redis.from_url(
        settings.valkey.url,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
        retry=retry,
        retry_on_error=[RedisTimeoutError, RedisConnectionError],
        health_check_interval=30,
    )


def check_valkey_connection() -> bool:
    """Check if Valkey is reachable.

    Uses a shorter timeout (5 seconds) than the main client since
    this is just a quick health check ping.

    Note:
        For new code, prefer using check_valkey_connection from
        infrastructure.cache which provides the same functionality.
    """
    from clickstream.infrastructure.cache import check_valkey_connection as _check

    return _check()


class SessionState:
    """
    Manages session state for a visitor in Valkey.

    Session state includes:
    - session_id: Unique session identifier (visitor_id + session_num)
    - session_start: First event timestamp in session
    - session_end: Latest event timestamp in session
    - event_count: Total events in session
    - view_count, cart_count, transaction_count: Event type counts
    - items_viewed, items_carted, items_purchased: Item lists
    - last_activity: Timestamp of last event (for session timeout)
    """

    # Key prefixes
    META_PREFIX = "session:meta:"

    def __init__(self, client: redis.Redis, timeout_minutes: int = 30, ttl_hours: int = 24):
        """
        Initialize session state manager.

        Args:
            client: Redis client
            timeout_minutes: Session inactivity timeout
            ttl_hours: TTL for session keys in Redis
        """
        self.client = client
        self.timeout_ms = timeout_minutes * 60 * 1000
        self.ttl_seconds = ttl_hours * 3600

    def _meta_key(self, visitor_id: int) -> str:
        return f"{self.META_PREFIX}{visitor_id}"

    def _execute_pipeline_with_retry(self, pipeline_builder) -> List:
        """
        Execute a Redis pipeline with retry logic for network resilience.

        Args:
            pipeline_builder: Callable that takes a pipeline and adds commands to it

        Returns:
            List of results from pipeline execution
        """
        from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
        from clickstream.utils.retry import RETRY_ATTEMPTS, RETRY_WAIT_MIN, RETRY_WAIT_MAX

        @retry(
            stop=stop_after_attempt(RETRY_ATTEMPTS),
            wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
            retry=retry_if_exception_type((RedisConnectionError, RedisTimeoutError)),
            reraise=True,
        )
        def _execute():
            pipe = self.client.pipeline()
            pipeline_builder(pipe)
            return pipe.execute()

        return _execute()

    def get_session(self, visitor_id: int) -> Optional[dict]:
        """
        Get current session state for a visitor.

        Returns:
            Session dict or None if no active session
        """
        meta_key = self._meta_key(visitor_id)
        data = self.client.hgetall(meta_key)

        if not data:
            return None

        # Parse the stored data
        return {
            "session_id": data.get("session_id"),
            "visitor_id": int(data.get("visitor_id", visitor_id)),
            "session_num": int(data.get("session_num", 1)),
            "session_start": int(data.get("session_start", 0)),
            "session_end": int(data.get("session_end", 0)),
            "event_count": int(data.get("event_count", 0)),
            "view_count": int(data.get("view_count", 0)),
            "cart_count": int(data.get("cart_count", 0)),
            "transaction_count": int(data.get("transaction_count", 0)),
            "items_viewed": json.loads(data.get("items_viewed", "[]")),
            "items_carted": json.loads(data.get("items_carted", "[]")),
            "items_purchased": json.loads(data.get("items_purchased", "[]")),
            "last_activity": int(data.get("last_activity", 0)),
        }

    def _is_session_expired(self, session: dict, event_timestamp: int) -> bool:
        """Check if session has expired based on inactivity timeout."""
        if not session or session.get("last_activity", 0) == 0:
            return True
        gap = event_timestamp - session["last_activity"]
        return gap > self.timeout_ms

    def update_session(self, event: dict) -> tuple[dict, bool]:
        """
        Update session state with a new event.

        Args:
            event: Event dictionary with timestamp, visitor_id, event, item_id, transaction_id

        Returns:
            Tuple of (session_dict, is_new_session)
        """
        visitor_id = event["visitor_id"]
        timestamp = event["timestamp"]
        event_type = event["event"]
        item_id = event["item_id"]

        meta_key = self._meta_key(visitor_id)

        # Get current session
        current = self.get_session(visitor_id)
        is_new_session = False

        # Check if we need a new session
        if current is None or self._is_session_expired(current, timestamp):
            is_new_session = True
            session_num = (current["session_num"] + 1) if current else 1
            session_id = f"{visitor_id}_{session_num}"

            current = {
                "session_id": session_id,
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

        # Update session with new event
        current["event_count"] += 1
        current["session_end"] = max(current["session_end"], timestamp)
        current["last_activity"] = timestamp

        # Update event type counts
        if event_type == "view":
            current["view_count"] += 1
            if item_id not in current["items_viewed"]:
                current["items_viewed"].append(item_id)
        elif event_type == "addtocart":
            current["cart_count"] += 1
            if item_id not in current["items_carted"]:
                current["items_carted"].append(item_id)
        elif event_type == "transaction":
            current["transaction_count"] += 1
            if item_id not in current["items_purchased"]:
                current["items_purchased"].append(item_id)

        # Save to Redis
        self.client.hset(
            meta_key,
            mapping={
                "session_id": current["session_id"],
                "visitor_id": str(current["visitor_id"]),
                "session_num": str(current["session_num"]),
                "session_start": str(current["session_start"]),
                "session_end": str(current["session_end"]),
                "event_count": str(current["event_count"]),
                "view_count": str(current["view_count"]),
                "cart_count": str(current["cart_count"]),
                "transaction_count": str(current["transaction_count"]),
                "items_viewed": json.dumps(current["items_viewed"]),
                "items_carted": json.dumps(current["items_carted"]),
                "items_purchased": json.dumps(current["items_purchased"]),
                "last_activity": str(current["last_activity"]),
            },
        )
        self.client.expire(meta_key, self.ttl_seconds)

        return current, is_new_session

    def batch_update_sessions(self, events: List[Dict]) -> List[Dict]:
        """
        Batch update sessions for multiple events using Redis pipelining.

        This is much faster than calling update_session() for each event
        when working with remote Redis/Valkey servers (e.g., Aiven).

        Performance: 2 Valkey round-trips instead of ~3500 for 1000 events.

        Includes retry logic with exponential backoff for network resilience.

        Args:
            events: List of event dicts with timestamp, visitor_id, event, item_id, transaction_id

        Returns:
            List of session dicts ready for to_db_record()
        """
        if not events:
            return []

        # 1. Get unique visitor_ids
        visitor_ids = list(set(e["visitor_id"] for e in events))

        # 2. Batch fetch all existing sessions (one pipeline call)
        # Wrap in retry logic for network resilience
        results = self._execute_pipeline_with_retry(
            lambda pipe: [pipe.hgetall(self._meta_key(vid)) for vid in visitor_ids]
        )

        # 3. Parse fetched sessions into a local dict
        sessions: Dict[int, Dict] = {}
        for vid, data in zip(visitor_ids, results):
            if data:
                sessions[vid] = {
                    "session_id": data.get("session_id"),
                    "visitor_id": int(data.get("visitor_id", vid)),
                    "session_num": int(data.get("session_num", 1)),
                    "session_start": int(data.get("session_start", 0)),
                    "session_end": int(data.get("session_end", 0)),
                    "event_count": int(data.get("event_count", 0)),
                    "view_count": int(data.get("view_count", 0)),
                    "cart_count": int(data.get("cart_count", 0)),
                    "transaction_count": int(data.get("transaction_count", 0)),
                    "items_viewed": json.loads(data.get("items_viewed", "[]")),
                    "items_carted": json.loads(data.get("items_carted", "[]")),
                    "items_purchased": json.loads(data.get("items_purchased", "[]")),
                    "last_activity": int(data.get("last_activity", 0)),
                }

        # 4. Sort events by timestamp to process in chronological order
        sorted_events = sorted(events, key=lambda e: e["timestamp"])

        # 5. Process all events locally (update sessions in memory)
        updated_visitor_ids = set()
        for event in sorted_events:
            visitor_id = event["visitor_id"]
            timestamp = event["timestamp"]
            event_type = event["event"]
            item_id = event["item_id"]

            current = sessions.get(visitor_id)

            # Check if we need a new session
            if current is None or self._is_session_expired(current, timestamp):
                session_num = (current["session_num"] + 1) if current else 1
                session_id = f"{visitor_id}_{session_num}"
                current = {
                    "session_id": session_id,
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
                sessions[visitor_id] = current

            # Update session with event
            current["event_count"] += 1
            current["session_end"] = max(current["session_end"], timestamp)
            current["last_activity"] = timestamp

            if event_type == "view":
                current["view_count"] += 1
                if item_id not in current["items_viewed"]:
                    current["items_viewed"].append(item_id)
            elif event_type == "addtocart":
                current["cart_count"] += 1
                if item_id not in current["items_carted"]:
                    current["items_carted"].append(item_id)
            elif event_type == "transaction":
                current["transaction_count"] += 1
                if item_id not in current["items_purchased"]:
                    current["items_purchased"].append(item_id)

            updated_visitor_ids.add(visitor_id)

        # 6. Batch write all updated sessions back to Valkey (one pipeline call)
        # Wrap in retry logic for network resilience
        def build_write_pipeline(pipe):
            for vid in updated_visitor_ids:
                session = sessions[vid]
                meta_key = self._meta_key(vid)
                pipe.hset(
                    meta_key,
                    mapping={
                        "session_id": session["session_id"],
                        "visitor_id": str(session["visitor_id"]),
                        "session_num": str(session["session_num"]),
                        "session_start": str(session["session_start"]),
                        "session_end": str(session["session_end"]),
                        "event_count": str(session["event_count"]),
                        "view_count": str(session["view_count"]),
                        "cart_count": str(session["cart_count"]),
                        "transaction_count": str(session["transaction_count"]),
                        "items_viewed": json.dumps(session["items_viewed"]),
                        "items_carted": json.dumps(session["items_carted"]),
                        "items_purchased": json.dumps(session["items_purchased"]),
                        "last_activity": str(session["last_activity"]),
                    },
                )
                pipe.expire(meta_key, self.ttl_seconds)

        self._execute_pipeline_with_retry(build_write_pipeline)

        # 7. Return updated sessions (already in memory, no fetch needed)
        return [sessions[vid] for vid in updated_visitor_ids]

    def to_db_record(self, session: dict) -> dict:
        """
        Convert session state to PostgreSQL record format.

        Args:
            session: Session dict from get_session() or update_session()

        Returns:
            Dict ready for PostgreSQL insert/upsert
        """
        # Convert timestamps to datetime
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


def clear_session_state() -> int:
    """
    Clear all session state from Valkey.

    Returns:
        Number of keys deleted
    """
    from clickstream.infrastructure.session_state import clear_session_state as _impl

    return _impl()


# ==============================================================================
# Producer Messages Counter (delegated to infrastructure.metrics)
# ==============================================================================

# Re-export key for backward compatibility
PRODUCER_MESSAGES_KEY = "producer:messages_produced"


def increment_producer_messages(count: int = 1) -> int:
    """Increment the producer messages counter."""
    from clickstream.infrastructure.metrics import increment_producer_messages as _impl

    return _impl(count)


def get_producer_messages() -> int:
    """Get the current producer messages count."""
    from clickstream.infrastructure.metrics import get_producer_messages as _impl

    return _impl()


def set_producer_messages(count: int) -> None:
    """Set the producer messages counter."""
    from clickstream.infrastructure.metrics import set_producer_messages as _impl

    _impl(count)


# ==============================================================================
# Throughput Stats Tracking (delegated to infrastructure.metrics)
# ==============================================================================

# Re-export keys for backward compatibility
STATS_SAMPLES_KEY_PREFIX = "clickstream:stats:samples:"
STATS_TTL_SECONDS = 3600  # 1 hour


def record_stats_sample(source: str, count: int, count2: Optional[int] = None) -> None:
    """Record a stats sample."""
    from clickstream.infrastructure.metrics import record_stats_sample as _impl

    _impl(source, count, count2)


def get_throughput_stats(source: str) -> dict:
    """Calculate throughput stats from stored samples."""
    from clickstream.infrastructure.metrics import get_throughput_stats as _impl

    return _impl(source)


# ==============================================================================
# Last Message Timestamp Tracking (delegated to infrastructure.metrics)
# ==============================================================================

# Re-export keys for backward compatibility
LAST_MESSAGE_TS_PREFIX = "consumer:last_message_ts:"
LAST_MESSAGE_TS_TTL_SECONDS = 86400  # 24 hours


def set_last_message_timestamp(group_id: str) -> None:
    """Store the current time as the last message processed timestamp."""
    from clickstream.infrastructure.metrics import set_last_message_timestamp as _impl

    _impl(group_id)


def get_last_message_timestamp(group_id: Optional[str] = None) -> Optional[datetime]:
    """Get the timestamp of the last processed message for a consumer group."""
    from clickstream.infrastructure.metrics import get_last_message_timestamp as _impl

    return _impl(group_id)


def get_kafka_consumer_lag(group_id: Optional[str] = None) -> Optional[int]:
    """
    Get the Kafka consumer lag (number of messages behind) for a consumer group.

    Args:
        group_id: Consumer group ID to check. If None, uses the default session consumer group.

    Returns:
        Number of messages the consumer is behind, or None if unavailable
    """
    from clickstream.infrastructure.kafka import get_consumer_lag

    return get_consumer_lag(group_id)
