# ==============================================================================
# Pipeline Metrics Infrastructure
# ==============================================================================
"""
Pipeline metrics tracking and monitoring.

This module provides:
- Producer message counters
- Throughput stats tracking (events/sec)
- Consumer lag time-series tracking

All metrics are stored in Valkey/Redis with appropriate TTLs.
"""

import json
import logging
import time

from clickstream.infrastructure.cache import ValkeyCache

logger = logging.getLogger(__name__)


# ==============================================================================
# Key Prefixes and TTLs
# ==============================================================================

PRODUCER_MESSAGES_KEY = "producer:messages_produced"
STATS_SAMPLES_KEY_PREFIX = "clickstream:stats:samples:"
LAG_SAMPLES_KEY_PREFIX = "clickstream:lag:samples:"
BACKPRESSURE_KEY_PREFIX = "clickstream:backpressure:"
STATS_TTL_SECONDS = 3600  # 1 hour
LAG_TTL_SECONDS = 3600  # 1 hour
BACKPRESSURE_TTL_SECONDS = 300  # 5 minutes


class PipelineMetrics:
    """
    Pipeline metrics manager using Valkey cache for storage.

    Tracks:
    - Producer message counts
    - Consumer throughput (events/sec)
    - Consumer lag time-series (per-partition offsets)
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

    def record_stats_sample(self, source: str, count: int, count2: int | None = None) -> None:
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
    # Consumer Lag Tracking
    # ==========================================================================

    def record_lag_sample(
        self,
        consumer_group: str,
        instance: int,
        partition_lags: dict[int, int],
    ) -> None:
        """
        Record a consumer lag sample in Valkey.

        Stores per-partition lag as a sorted-set entry keyed by timestamp.
        Each consumer instance records its own lag independently.

        Args:
            consumer_group: Kafka consumer group ID (e.g., "clickstream-postgresql")
            instance: Consumer instance number (0-indexed)
            partition_lags: Mapping of partition index to lag (messages behind)
        """
        timestamp = int(time.time())
        key = f"{LAG_SAMPLES_KEY_PREFIX}{consumer_group}:{instance}"

        sample_data = {
            "ts": timestamp,
            "instance": instance,
            "partitions": partition_lags,
            "total": sum(partition_lags.values()),
        }
        sample = json.dumps(sample_data)

        self.client.zadd(key, {sample: timestamp})
        self.client.expire(key, LAG_TTL_SECONDS)

        # Clean up old samples
        cutoff = timestamp - LAG_TTL_SECONDS
        self.client.zremrangebyscore(key, "-inf", cutoff)

    def get_lag_history(
        self,
        consumer_group: str,
        window_seconds: int = 300,
    ) -> list[dict]:
        """
        Get lag history across all consumer instances.

        Retrieves lag samples from all instances within the time window,
        merges them by timestamp, and returns a unified per-partition view.

        Args:
            consumer_group: Kafka consumer group ID
            window_seconds: How far back to look (default: 5 minutes)

        Returns:
            List of dicts sorted by timestamp, each containing:
            - ts: Unix timestamp
            - partitions: {partition_index: lag}
            - total: Total lag across all partitions
        """
        # Discover all instance keys for this consumer group
        pattern = f"{LAG_SAMPLES_KEY_PREFIX}{consumer_group}:*"
        keys = []
        cursor = 0
        while True:
            cursor, batch = self.client.scan(cursor, match=pattern, count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        if not keys:
            return []

        cutoff = int(time.time()) - window_seconds

        # Collect samples from all instances
        # Group by timestamp so we merge partitions from different instances
        samples_by_ts: dict[int, dict[int, int]] = {}

        for key in keys:
            raw = self.client.zrangebyscore(key, cutoff, "+inf", withscores=True)
            if not raw:
                continue
            for sample_json, _score in raw:
                try:
                    sample = json.loads(sample_json)
                except json.JSONDecodeError:
                    continue
                ts = sample["ts"]
                partitions = sample.get("partitions", {})
                if ts not in samples_by_ts:
                    samples_by_ts[ts] = {}
                # Merge partitions (each instance reports different partitions)
                for part_str, lag in partitions.items():
                    samples_by_ts[ts][int(part_str)] = lag

        # Build sorted result
        result = []
        for ts in sorted(samples_by_ts):
            partitions = samples_by_ts[ts]
            result.append(
                {
                    "ts": ts,
                    "partitions": dict(sorted(partitions.items())),
                    "total": sum(partitions.values()),
                }
            )

        return result

    def get_lag_trend(
        self,
        consumer_group: str,
        window_seconds: int = 120,
    ) -> str:
        """
        Determine whether consumer lag is growing, stable, or shrinking.

        Uses linear regression over recent lag samples to compute a trend.
        The slope is compared against the mean lag to determine significance.

        Args:
            consumer_group: Kafka consumer group ID
            window_seconds: Time window for trend analysis (default: 2 minutes)

        Returns:
            One of: "growing", "stable", "shrinking", or "unknown"
        """
        history = self.get_lag_history(consumer_group, window_seconds)

        if len(history) < 2:
            return "unknown"

        # Extract (timestamp, total_lag) pairs
        points = [(s["ts"], s["total"]) for s in history]

        # Simple linear regression: slope = correlation * (std_y / std_x)
        n = len(points)
        sum_x = sum(t for t, _ in points)
        sum_y = sum(lag for _, lag in points)
        sum_xy = sum(t * lag for t, lag in points)
        sum_x2 = sum(t * t for t, _ in points)

        denominator = n * sum_x2 - sum_x * sum_x
        if denominator == 0:
            return "unknown"

        slope = (n * sum_xy - sum_x * sum_y) / denominator

        # Determine significance relative to mean lag
        mean_lag = sum_y / n
        if mean_lag == 0:
            # No lag at all
            return "stable" if abs(slope) < 1 else ("growing" if slope > 0 else "shrinking")

        # Slope as a fraction of mean lag per second
        # > 1% of mean lag per second = significant trend
        relative_slope = abs(slope) / mean_lag
        if relative_slope < 0.01:
            return "stable"
        elif slope > 0:
            return "growing"
        else:
            return "shrinking"

    # ==========================================================================
    # Backpressure Indicator Tracking
    # ==========================================================================

    def record_backpressure_sample(
        self,
        consumer_group: str,
        instance: int,
        indicators: dict,
    ) -> None:
        """
        Record a backpressure indicators sample in Valkey.

        Called from the consumer's periodic summary callback to push the
        latest BatchMetrics indicators into Valkey for CLI retrieval.

        Args:
            consumer_group: Kafka consumer group ID (e.g., "clickstream-postgresql")
            instance: Consumer instance number (0-indexed)
            indicators: Dict with keys:
                - fill_ratio: float (0.0–1.0)
                - poll_proximity: float (0.0–1.0+)
                - idle_ms: float
                - rebalance_count: int (last 5 minutes)
                - bottleneck_stage: str | None (e.g., "valkey (↑ 48%)")
        """
        timestamp = int(time.time())
        key = f"{BACKPRESSURE_KEY_PREFIX}{consumer_group}:{instance}"

        sample_data = {
            "ts": timestamp,
            "instance": instance,
            **indicators,
        }
        sample = json.dumps(sample_data)

        # Use a sorted set so we can retrieve the latest by score
        self.client.zadd(key, {sample: timestamp})
        self.client.expire(key, BACKPRESSURE_TTL_SECONDS)

        # Clean up old samples (keep only last 5 minutes)
        cutoff = timestamp - BACKPRESSURE_TTL_SECONDS
        self.client.zremrangebyscore(key, "-inf", cutoff)

    def get_backpressure_report(
        self,
        consumer_group: str,
    ) -> dict | None:
        """
        Get the latest backpressure indicators across all consumer instances.

        Discovers all instance keys for the consumer group, retrieves the
        most recent sample from each, and aggregates them into a single
        report (using worst-case values for saturation indicators).

        Args:
            consumer_group: Kafka consumer group ID

        Returns:
            Dict with aggregated indicators, or None if no data:
            - fill_ratio: float (max across instances)
            - poll_proximity: float (max across instances)
            - idle_ms: float (min across instances — least headroom)
            - rebalance_count: int (sum across instances)
            - bottleneck_stage: str | None (from instance with highest proximity)
            - instance_count: int
            - ts: int (most recent timestamp)
        """
        # Discover all instance keys for this consumer group
        pattern = f"{BACKPRESSURE_KEY_PREFIX}{consumer_group}:*"
        keys = []
        cursor = 0
        while True:
            cursor, batch = self.client.scan(cursor, match=pattern, count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        if not keys:
            return None

        # Get the most recent sample from each instance
        latest_samples = []
        for key in keys:
            # Get the last entry (highest score)
            entries = self.client.zrange(key, -1, -1, withscores=True)
            if entries:
                sample_json, _score = entries[0]
                try:
                    sample = json.loads(sample_json)
                    latest_samples.append(sample)
                except json.JSONDecodeError:
                    continue

        if not latest_samples:
            return None

        # Aggregate across instances (worst-case for saturation indicators)
        max_fill = max(s.get("fill_ratio", 0.0) for s in latest_samples)
        max_proximity = max(s.get("poll_proximity", 0.0) for s in latest_samples)
        min_idle = min(s.get("idle_ms", 0.0) for s in latest_samples)
        total_rebalances = sum(s.get("rebalance_count", 0) for s in latest_samples)
        latest_ts = max(s.get("ts", 0) for s in latest_samples)

        # Bottleneck stage from the instance with highest proximity
        worst_instance = max(latest_samples, key=lambda s: s.get("poll_proximity", 0.0))
        bottleneck = worst_instance.get("bottleneck_stage")

        return {
            "fill_ratio": max_fill,
            "poll_proximity": max_proximity,
            "idle_ms": min_idle,
            "rebalance_count": total_rebalances,
            "bottleneck_stage": bottleneck,
            "instance_count": len(latest_samples),
            "ts": latest_ts,
        }


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


def record_stats_sample(source: str, count: int, count2: int | None = None) -> None:
    """Record a stats sample."""
    _get_metrics().record_stats_sample(source, count, count2)


def get_throughput_stats(source: str) -> dict:
    """Calculate throughput stats."""
    return _get_metrics().get_throughput_stats(source)


def record_lag_sample(consumer_group: str, instance: int, partition_lags: dict[int, int]) -> None:
    """Record a consumer lag sample."""
    _get_metrics().record_lag_sample(consumer_group, instance, partition_lags)


def get_lag_history(consumer_group: str, window_seconds: int = 300) -> list[dict]:
    """Get lag history across all consumer instances."""
    return _get_metrics().get_lag_history(consumer_group, window_seconds)


def get_lag_trend(consumer_group: str, window_seconds: int = 120) -> str:
    """Determine whether consumer lag is growing, stable, or shrinking."""
    return _get_metrics().get_lag_trend(consumer_group, window_seconds)


def record_backpressure_sample(consumer_group: str, instance: int, indicators: dict) -> None:
    """Record a backpressure indicators sample."""
    _get_metrics().record_backpressure_sample(consumer_group, instance, indicators)


def get_backpressure_report(consumer_group: str) -> dict | None:
    """Get the latest backpressure indicators across all consumer instances."""
    return _get_metrics().get_backpressure_report(consumer_group)
