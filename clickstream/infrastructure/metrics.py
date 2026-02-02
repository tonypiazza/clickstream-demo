# ==============================================================================
# Pipeline Metrics Infrastructure
# ==============================================================================
"""
Pipeline metrics tracking and monitoring.

This module provides:
- Producer message counters
- Throughput stats tracking (events/sec)
- Consumer lag monitoring
- Last message timestamp tracking

All metrics are stored in Valkey/Redis with appropriate TTLs.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from clickstream.infrastructure.cache import ValkeyCache
from clickstream.utils.config import get_settings

logger = logging.getLogger(__name__)


# ==============================================================================
# Key Prefixes and TTLs
# ==============================================================================

PRODUCER_MESSAGES_KEY = "producer:messages_produced"
STATS_SAMPLES_KEY_PREFIX = "clickstream:stats:samples:"
STATS_TTL_SECONDS = 3600  # 1 hour
LAST_MESSAGE_TS_PREFIX = "consumer:last_message_ts:"
LAST_MESSAGE_TS_TTL_SECONDS = 86400  # 24 hours


class PipelineMetrics:
    """
    Pipeline metrics manager using Valkey cache for storage.

    Tracks:
    - Producer message counts
    - Consumer throughput (events/sec)
    - Consumer lag
    - Last message timestamps
    """

    def __init__(self, cache: ValkeyCache | None = None):
        """
        Initialize pipeline metrics.

        Args:
            cache: ValkeyCache instance. If None, creates a new one.
        """
        self._cache = cache or ValkeyCache()

    @property
    def client(self):
        """Get the underlying Redis client for direct operations."""
        return self._cache.client

    # ==========================================================================
    # Producer Message Tracking
    # ==========================================================================

    def increment_producer_messages(self, count: int = 1) -> int:
        """
        Increment the producer messages counter.

        Args:
            count: Number to increment by (default: 1)

        Returns:
            New total count after increment
        """
        return self._cache.increment(PRODUCER_MESSAGES_KEY, count)

    def get_producer_messages(self) -> int:
        """
        Get the current producer messages count.

        Returns:
            Current count, or 0 if not set
        """
        value = self.client.get(PRODUCER_MESSAGES_KEY)
        return int(value) if value else 0

    def set_producer_messages(self, count: int) -> None:
        """
        Set the producer messages counter (replaces previous value).

        Used to track the number of messages from the most recent producer run.

        Args:
            count: Total messages produced in the current run
        """
        self.client.set(PRODUCER_MESSAGES_KEY, count)

    # ==========================================================================
    # Throughput Stats Tracking
    # ==========================================================================

    def record_stats_sample(self, source: str, count: int, count2: Optional[int] = None) -> None:
        """
        Record a stats sample in Valkey.

        Args:
            source: Source identifier (e.g., "postgresql", "opensearch")
            count: Primary count (e.g., events for PostgreSQL, documents for OpenSearch)
            count2: Optional secondary count (e.g., sessions for PostgreSQL)
        """
        timestamp = int(time.time())
        key = f"{STATS_SAMPLES_KEY_PREFIX}{source}"

        # Store as JSON with timestamp as score
        sample_data = {"count": count, "ts": timestamp}
        if count2 is not None:
            sample_data["count2"] = count2
        sample = json.dumps(sample_data)

        # Add to sorted set
        self.client.zadd(key, {sample: timestamp})

        # Set TTL on the key
        self.client.expire(key, STATS_TTL_SECONDS)

        # Clean up old samples (older than 1 hour)
        cutoff = timestamp - STATS_TTL_SECONDS
        self.client.zremrangebyscore(key, "-inf", cutoff)

    def get_throughput_stats(self, source: str) -> dict:
        """
        Calculate throughput stats from stored samples.

        Args:
            source: Source identifier (e.g., "postgresql", "opensearch")

        Returns:
            Dict with:
            - current_rate: count/sec over last 60 seconds
            - samples_count: number of samples in window
        """
        key = f"{STATS_SAMPLES_KEY_PREFIX}{source}"

        # Get all samples
        samples_raw = self.client.zrange(key, 0, -1, withscores=True)

        if not samples_raw or len(samples_raw) < 2:
            return {
                "current_rate": None,
                "samples_count": len(samples_raw) if samples_raw else 0,
            }

        # Parse samples and sort by timestamp
        samples = []
        for sample_json, score in samples_raw:
            try:
                sample = json.loads(sample_json)
                samples.append(sample)
            except json.JSONDecodeError:
                continue

        samples.sort(key=lambda x: x["ts"])

        if len(samples) < 2:
            return {
                "current_rate": None,
                "samples_count": len(samples),
            }

        # Calculate current rate (last 60 seconds)
        now = int(time.time())
        recent_samples = [s for s in samples if now - s["ts"] <= 60]

        current_rate = None
        if len(recent_samples) >= 2:
            oldest = recent_samples[0]
            newest = recent_samples[-1]
            time_diff = newest["ts"] - oldest["ts"]
            if time_diff > 0:
                count_diff = newest["count"] - oldest["count"]
                current_rate = count_diff / time_diff

        return {
            "current_rate": current_rate,
            "samples_count": len(samples),
        }

    # ==========================================================================
    # Last Message Timestamp Tracking
    # ==========================================================================

    def set_last_message_timestamp(self, group_id: str) -> None:
        """
        Store the current time as the last message processed timestamp.

        Args:
            group_id: Consumer group ID to store timestamp for
        """
        key = f"{LAST_MESSAGE_TS_PREFIX}{group_id}"
        self.client.set(key, datetime.now(timezone.utc).isoformat())
        self.client.expire(key, LAST_MESSAGE_TS_TTL_SECONDS)

    def get_last_message_timestamp(self, group_id: Optional[str] = None) -> Optional[datetime]:
        """
        Get the timestamp of the last processed message for a consumer group.

        Args:
            group_id: Consumer group ID to check. If None, uses the default consumer group.

        Returns:
            datetime of last message processed (in local time), or None if unavailable
        """
        try:
            settings = get_settings()
            effective_group_id = group_id if group_id else settings.postgresql_consumer.group_id
            key = f"{LAST_MESSAGE_TS_PREFIX}{effective_group_id}"
            value = self.client.get(key)
            if value:
                # Parse UTC timestamp and convert to local time
                utc_time = datetime.fromisoformat(value)
                return utc_time.replace(tzinfo=timezone.utc).astimezone()
            return None
        except Exception:
            return None


# ==============================================================================
# Module-level convenience functions (for backward compatibility)
# ==============================================================================

_default_metrics: PipelineMetrics | None = None


def _get_metrics() -> PipelineMetrics:
    """Get or create the default PipelineMetrics instance."""
    global _default_metrics
    if _default_metrics is None:
        _default_metrics = PipelineMetrics()
    return _default_metrics


def increment_producer_messages(count: int = 1) -> int:
    """Increment the producer messages counter."""
    return _get_metrics().increment_producer_messages(count)


def get_producer_messages() -> int:
    """Get the current producer messages count."""
    return _get_metrics().get_producer_messages()


def set_producer_messages(count: int) -> None:
    """Set the producer messages counter."""
    _get_metrics().set_producer_messages(count)


def record_stats_sample(source: str, count: int, count2: Optional[int] = None) -> None:
    """Record a stats sample."""
    _get_metrics().record_stats_sample(source, count, count2)


def get_throughput_stats(source: str) -> dict:
    """Calculate throughput stats."""
    return _get_metrics().get_throughput_stats(source)


def set_last_message_timestamp(group_id: str) -> None:
    """Store the last message processed timestamp."""
    _get_metrics().set_last_message_timestamp(group_id)


def get_last_message_timestamp(group_id: Optional[str] = None) -> Optional[datetime]:
    """Get the last message processed timestamp."""
    return _get_metrics().get_last_message_timestamp(group_id)
