# ==============================================================================
# Session State Store Implementation (Valkey/Redis)
# ==============================================================================
"""
Valkey/Redis implementation of the SessionStateStore interface.

This module provides session state persistence for streaming pipelines,
storing session data as Redis hashes with configurable TTLs.
"""

import json
import logging
from collections.abc import Callable

import redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from clickstream.base.session_state import SessionStateStore
from clickstream.utils.config import get_settings
from clickstream.utils.retry import RETRY_ATTEMPTS, RETRY_WAIT_MAX, RETRY_WAIT_MIN, VALKEY_RETRIES

logger = logging.getLogger(__name__)


# Key prefix for session metadata
SESSION_META_PREFIX = "session:meta:"


def get_valkey_client() -> redis.Redis:
    """
    Get a Valkey/Redis client connection.

    Configured with:
    - 10 second socket timeouts for fast failure detection
    - 10 automatic retries with exponential backoff for transient failures
    - Health check interval to keep connections alive

    Returns:
        redis.Redis client instance
    """
    settings = get_settings()

    # Configure retry with exponential backoff for transient failures
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


class ValkeySessionStateStore(SessionStateStore):
    """
    Valkey/Redis implementation of SessionStateStore.

    Stores session state as Redis hashes with the key format:
    session:meta:{visitor_id}

    Each hash contains:
    - session_id: Unique session identifier
    - visitor_id: Visitor ID
    - session_num: Session number for this visitor
    - session_start: First event timestamp (ms)
    - session_end: Last event timestamp (ms)
    - event_count: Total events in session
    - view_count, cart_count, transaction_count: Event type counts
    - items_viewed, items_carted, items_purchased: JSON arrays of item IDs
    - last_activity: Timestamp of last activity (ms)
    """

    def __init__(self, client: redis.Redis | None = None):
        """
        Initialize the session state store.

        Args:
            client: Redis client instance. If None, creates a new connection.
        """
        self._client = client or get_valkey_client()

    @property
    def client(self) -> redis.Redis:
        """Get the underlying Redis client."""
        return self._client

    def _meta_key(self, visitor_id: int) -> str:
        """Generate the Redis key for a visitor's session metadata."""
        return f"{SESSION_META_PREFIX}{visitor_id}"

    def _parse_session(self, visitor_id: int, data: dict) -> dict:
        """Parse raw Redis hash data into a session dict."""
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

    def _serialize_session(self, session: dict) -> dict:
        """Serialize a session dict for Redis hash storage."""
        return {
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
        }

    def _execute_pipeline_with_retry(self, pipeline_builder: Callable) -> list:
        """
        Execute a Redis pipeline with retry logic for network resilience.

        Args:
            pipeline_builder: Callable that takes a pipeline and adds commands to it

        Returns:
            List of results from pipeline execution
        """
        from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

        @retry(
            stop=stop_after_attempt(RETRY_ATTEMPTS),
            wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
            retry=retry_if_exception_type((RedisConnectionError, RedisTimeoutError)),
            reraise=True,
        )
        def _execute():
            pipe = self._client.pipeline()
            pipeline_builder(pipe)
            return pipe.execute()

        return _execute()

    # ==========================================================================
    # SessionStateStore Interface Implementation
    # ==========================================================================

    def get_sessions(self, visitor_ids: list[int]) -> dict[int, dict]:
        """
        Get session states for multiple visitors using pipelining.

        Args:
            visitor_ids: List of visitor IDs to fetch

        Returns:
            Dict mapping visitor_id to session dict.
            Missing visitors are omitted from the result.
        """
        if not visitor_ids:
            return {}

        # Batch fetch using pipeline
        results = self._execute_pipeline_with_retry(
            lambda pipe: [pipe.hgetall(self._meta_key(vid)) for vid in visitor_ids]
        )

        # Parse results
        sessions: dict[int, dict] = {}
        for vid, data in zip(visitor_ids, results):
            if data:
                sessions[vid] = self._parse_session(vid, data)

        return sessions

    def save_sessions(self, sessions: dict[int, dict], ttl_seconds: int) -> None:
        """
        Save session states with TTL using pipelining.

        Args:
            sessions: Dict mapping visitor_id to session dict
            ttl_seconds: Time-to-live for the cached sessions
        """
        if not sessions:
            return

        def build_pipeline(pipe):
            for vid, session in sessions.items():
                meta_key = self._meta_key(vid)
                pipe.hset(meta_key, mapping=self._serialize_session(session))
                pipe.expire(meta_key, ttl_seconds)

        self._execute_pipeline_with_retry(build_pipeline)

    def clear_all(self) -> int:
        """
        Clear all session state.

        Returns:
            Count of sessions deleted
        """
        keys = list(self._client.scan_iter(f"{SESSION_META_PREFIX}*"))

        if keys:
            return self._client.delete(*keys)
        return 0

    # ==========================================================================
    # Additional Methods (beyond ABC)
    # ==========================================================================

    def get_session(self, visitor_id: int) -> dict | None:
        """
        Get session state for a single visitor.

        Args:
            visitor_id: Visitor ID to fetch

        Returns:
            Session dict or None if no session exists
        """
        data = self._client.hgetall(self._meta_key(visitor_id))
        if not data:
            return None
        return self._parse_session(visitor_id, data)

    def save_session(self, visitor_id: int, session: dict, ttl_seconds: int) -> None:
        """
        Save session state for a single visitor.

        Args:
            visitor_id: Visitor ID
            session: Session dict to save
            ttl_seconds: Time-to-live for the cached session
        """
        meta_key = self._meta_key(visitor_id)
        self._client.hset(meta_key, mapping=self._serialize_session(session))
        self._client.expire(meta_key, ttl_seconds)


# ==============================================================================
# Module-level convenience functions
# ==============================================================================

_default_store: ValkeySessionStateStore | None = None


def _get_store() -> ValkeySessionStateStore:
    """Get or create the default ValkeySessionStateStore instance."""
    global _default_store
    if _default_store is None:
        _default_store = ValkeySessionStateStore()
    return _default_store


def get_sessions(visitor_ids: list[int]) -> dict[int, dict]:
    """Get session states for multiple visitors."""
    return _get_store().get_sessions(visitor_ids)


def save_sessions(sessions: dict[int, dict], ttl_seconds: int) -> None:
    """Save session states with TTL."""
    _get_store().save_sessions(sessions, ttl_seconds)


def clear_session_state() -> int:
    """Clear all session state."""
    return _get_store().clear_all()
