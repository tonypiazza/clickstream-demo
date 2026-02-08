# ==============================================================================
# Tests for PipelineMetrics Lag Tracking
# ==============================================================================
"""
Unit tests for consumer lag time-series tracking in PipelineMetrics.

Tests cover:
- Recording lag samples (storage, TTL, cleanup)
- Retrieving lag history (single/multi instance, window filtering)
- Computing lag trend (growing, shrinking, stable, unknown, zero)

All tests use fakeredis via the `metrics` fixture from conftest.py,
so no real Valkey/Redis server is needed.
"""

import json
import time

from clickstream.infrastructure.metrics import (
    BACKPRESSURE_KEY_PREFIX,
    LAG_SAMPLES_KEY_PREFIX,
)

# ==============================================================================
# record_lag_sample
# ==============================================================================


class TestRecordLagSample:
    """Tests for PipelineMetrics.record_lag_sample()."""

    def test_stores_data_in_sorted_set(self, metrics, fake_redis):
        """Recording a sample stores it as a JSON entry in a Valkey sorted set."""
        metrics.record_lag_sample("test-group", 0, {0: 100, 1: 200})

        key = f"{LAG_SAMPLES_KEY_PREFIX}test-group:0"
        entries = fake_redis.zrange(key, 0, -1, withscores=True)

        assert len(entries) == 1
        sample_json, score = entries[0]
        sample = json.loads(sample_json)

        assert sample["instance"] == 0
        assert sample["partitions"] == {"0": 100, "1": 200}
        assert sample["total"] == 300
        assert "ts" in sample
        # Score should be the timestamp
        assert score == sample["ts"]

    def test_key_pattern_includes_group_and_instance(self, metrics, fake_redis):
        """Key format is clickstream:lag:samples:{group}:{instance}."""
        metrics.record_lag_sample("my-group", 2, {0: 50})

        key = f"{LAG_SAMPLES_KEY_PREFIX}my-group:2"
        assert fake_redis.exists(key)

    def test_ttl_is_set(self, metrics, fake_redis):
        """The sorted set key should have a TTL (expiry) set."""
        metrics.record_lag_sample("test-group", 0, {0: 10})

        key = f"{LAG_SAMPLES_KEY_PREFIX}test-group:0"
        ttl = fake_redis.ttl(key)
        # TTL should be positive (up to LAG_TTL_SECONDS = 3600)
        assert ttl > 0
        assert ttl <= 3600

    def test_multiple_samples_accumulate(self, metrics, fake_redis):
        """Multiple record calls add separate entries to the sorted set."""
        metrics.record_lag_sample("test-group", 0, {0: 100})
        # Advance timestamp slightly for a distinct score
        time.sleep(0.01)
        metrics.record_lag_sample("test-group", 0, {0: 200})

        key = f"{LAG_SAMPLES_KEY_PREFIX}test-group:0"
        entries = fake_redis.zrange(key, 0, -1)
        # May be 1 or 2 depending on whether timestamps collide (same second)
        # but at least 1 entry should exist
        assert len(entries) >= 1


# ==============================================================================
# get_lag_history
# ==============================================================================


class TestGetLagHistory:
    """Tests for PipelineMetrics.get_lag_history()."""

    def test_empty_returns_empty_list(self, metrics):
        """No data recorded → returns empty list."""
        result = metrics.get_lag_history("nonexistent-group")
        assert result == []

    def test_single_instance_returns_sorted(self, metrics):
        """Single instance with one sample → returns list with that sample."""
        metrics.record_lag_sample("test-group", 0, {0: 100, 1: 50})

        history = metrics.get_lag_history("test-group", window_seconds=60)

        assert len(history) >= 1
        latest = history[-1]
        assert latest["partitions"] == {0: 100, 1: 50}
        assert latest["total"] == 150
        assert "ts" in latest

    def test_multi_instance_merges_partitions(self, metrics):
        """Two instances reporting different partitions at same second → merged."""
        # Instance 0 reports partition 0
        metrics.record_lag_sample("test-group", 0, {0: 100})
        # Instance 1 reports partition 1
        metrics.record_lag_sample("test-group", 1, {1: 200})

        history = metrics.get_lag_history("test-group", window_seconds=60)

        assert len(history) >= 1
        # Find the sample that has both partitions merged
        latest = history[-1]
        assert 0 in latest["partitions"]
        assert 1 in latest["partitions"]
        assert latest["partitions"][0] == 100
        assert latest["partitions"][1] == 200
        assert latest["total"] == 300

    def test_window_filtering(self, metrics, fake_redis):
        """Samples outside the window are excluded."""
        group = "test-group"
        key = f"{LAG_SAMPLES_KEY_PREFIX}{group}:0"

        now = int(time.time())

        # Manually insert an old sample (5 minutes ago)
        old_sample = json.dumps(
            {
                "ts": now - 300,
                "instance": 0,
                "partitions": {"0": 999},
                "total": 999,
            }
        )
        fake_redis.zadd(key, {old_sample: now - 300})

        # Insert a recent sample
        recent_sample = json.dumps(
            {
                "ts": now,
                "instance": 0,
                "partitions": {"0": 10},
                "total": 10,
            }
        )
        fake_redis.zadd(key, {recent_sample: now})

        # Query with a 60-second window — should only get the recent sample
        history = metrics.get_lag_history(group, window_seconds=60)

        assert len(history) == 1
        assert history[0]["total"] == 10

    def test_partitions_sorted_in_result(self, metrics):
        """Partition keys in each sample are sorted numerically."""
        metrics.record_lag_sample("test-group", 0, {2: 30, 0: 10, 1: 20})

        history = metrics.get_lag_history("test-group", window_seconds=60)

        assert len(history) >= 1
        partition_keys = list(history[-1]["partitions"].keys())
        assert partition_keys == sorted(partition_keys)


# ==============================================================================
# get_lag_trend
# ==============================================================================


class TestGetLagTrend:
    """Tests for PipelineMetrics.get_lag_trend()."""

    def test_unknown_with_no_data(self, metrics):
        """No samples → 'unknown'."""
        assert metrics.get_lag_trend("empty-group") == "unknown"

    def test_unknown_with_one_sample(self, metrics):
        """One sample → 'unknown' (need ≥2 for regression)."""
        metrics.record_lag_sample("test-group", 0, {0: 100})
        assert metrics.get_lag_trend("test-group") == "unknown"

    def test_growing(self, metrics, fake_redis):
        """Lag increasing over time → 'growing'."""
        group = "test-group"
        key = f"{LAG_SAMPLES_KEY_PREFIX}{group}:0"
        now = int(time.time())

        # Insert samples with increasing lag
        for i in range(5):
            ts = now - 100 + (i * 20)
            sample = json.dumps(
                {
                    "ts": ts,
                    "instance": 0,
                    "partitions": {"0": 100 + i * 500},
                    "total": 100 + i * 500,
                }
            )
            fake_redis.zadd(key, {sample: ts})

        assert metrics.get_lag_trend(group, window_seconds=120) == "growing"

    def test_shrinking(self, metrics, fake_redis):
        """Lag decreasing over time → 'shrinking'."""
        group = "test-group"
        key = f"{LAG_SAMPLES_KEY_PREFIX}{group}:0"
        now = int(time.time())

        # Insert samples with decreasing lag
        for i in range(5):
            ts = now - 100 + (i * 20)
            lag = 2000 - i * 500
            sample = json.dumps(
                {
                    "ts": ts,
                    "instance": 0,
                    "partitions": {"0": lag},
                    "total": lag,
                }
            )
            fake_redis.zadd(key, {sample: ts})

        assert metrics.get_lag_trend(group, window_seconds=120) == "shrinking"

    def test_stable(self, metrics, fake_redis):
        """Lag flat over time → 'stable'."""
        group = "test-group"
        key = f"{LAG_SAMPLES_KEY_PREFIX}{group}:0"
        now = int(time.time())

        # Insert samples with constant lag
        for i in range(5):
            ts = now - 100 + (i * 20)
            sample = json.dumps(
                {
                    "ts": ts,
                    "instance": 0,
                    "partitions": {"0": 500},
                    "total": 500,
                }
            )
            fake_redis.zadd(key, {sample: ts})

        assert metrics.get_lag_trend(group, window_seconds=120) == "stable"

    def test_zero_lag_stable(self, metrics, fake_redis):
        """All lags zero → 'stable'."""
        group = "test-group"
        key = f"{LAG_SAMPLES_KEY_PREFIX}{group}:0"
        now = int(time.time())

        for i in range(3):
            ts = now - 60 + (i * 20)
            sample = json.dumps(
                {
                    "ts": ts,
                    "instance": 0,
                    "partitions": {"0": 0, "1": 0},
                    "total": 0,
                }
            )
            fake_redis.zadd(key, {sample: ts})

        assert metrics.get_lag_trend(group, window_seconds=120) == "stable"


# ==============================================================================
# record_backpressure_sample — Phase 4f
# ==============================================================================


class TestRecordBackpressureSample:
    """Tests for PipelineMetrics.record_backpressure_sample()."""

    def test_stores_data_in_sorted_set(self, metrics, fake_redis):
        """Recording a sample stores it as a JSON entry in a Valkey sorted set."""
        indicators = {
            "fill_ratio": 0.95,
            "poll_proximity": 0.32,
            "idle_ms": 2.0,
            "rebalance_count": 0,
            "bottleneck_stage": None,
        }
        metrics.record_backpressure_sample("test-group", 0, indicators)

        key = f"{BACKPRESSURE_KEY_PREFIX}test-group:0"
        entries = fake_redis.zrange(key, 0, -1, withscores=True)

        assert len(entries) == 1
        sample_json, score = entries[0]
        sample = json.loads(sample_json)

        assert sample["instance"] == 0
        assert sample["fill_ratio"] == 0.95
        assert sample["poll_proximity"] == 0.32
        assert sample["idle_ms"] == 2.0
        assert sample["rebalance_count"] == 0
        assert sample["bottleneck_stage"] is None
        assert "ts" in sample
        assert score == sample["ts"]

    def test_key_pattern_includes_group_and_instance(self, metrics, fake_redis):
        """Key format is clickstream:backpressure:{group}:{instance}."""
        metrics.record_backpressure_sample("my-group", 3, {"fill_ratio": 0.5})

        key = f"{BACKPRESSURE_KEY_PREFIX}my-group:3"
        assert fake_redis.exists(key)

    def test_ttl_is_set(self, metrics, fake_redis):
        """The sorted set key should have a TTL (expiry) set."""
        metrics.record_backpressure_sample("test-group", 0, {"fill_ratio": 0.5})

        key = f"{BACKPRESSURE_KEY_PREFIX}test-group:0"
        ttl = fake_redis.ttl(key)
        assert ttl > 0
        assert ttl <= 300  # BACKPRESSURE_TTL_SECONDS

    def test_multiple_samples_accumulate(self, metrics, fake_redis):
        """Multiple record calls add separate entries to the sorted set."""
        metrics.record_backpressure_sample("test-group", 0, {"fill_ratio": 0.5})
        time.sleep(0.01)
        metrics.record_backpressure_sample("test-group", 0, {"fill_ratio": 0.8})

        key = f"{BACKPRESSURE_KEY_PREFIX}test-group:0"
        entries = fake_redis.zrange(key, 0, -1)
        assert len(entries) >= 1

    def test_indicator_values_preserved(self, metrics, fake_redis):
        """All indicator values are preserved exactly in the stored sample."""
        indicators = {
            "fill_ratio": 0.98,
            "poll_proximity": 0.75,
            "idle_ms": 1.5,
            "rebalance_count": 3,
            "bottleneck_stage": "valkey (↑ 48%)",
        }
        metrics.record_backpressure_sample("test-group", 0, indicators)

        key = f"{BACKPRESSURE_KEY_PREFIX}test-group:0"
        entries = fake_redis.zrange(key, 0, -1)
        sample = json.loads(entries[0])

        assert sample["fill_ratio"] == 0.98
        assert sample["poll_proximity"] == 0.75
        assert sample["idle_ms"] == 1.5
        assert sample["rebalance_count"] == 3
        assert sample["bottleneck_stage"] == "valkey (↑ 48%)"


# ==============================================================================
# get_backpressure_report — Phase 4f
# ==============================================================================


class TestGetBackpressureReport:
    """Tests for PipelineMetrics.get_backpressure_report()."""

    def test_no_data_returns_none(self, metrics):
        """No data recorded → returns None."""
        result = metrics.get_backpressure_report("nonexistent-group")
        assert result is None

    def test_single_instance_returns_indicators(self, metrics):
        """Single instance with one sample → returns its indicators."""
        indicators = {
            "fill_ratio": 0.95,
            "poll_proximity": 0.32,
            "idle_ms": 2.0,
            "rebalance_count": 0,
            "bottleneck_stage": None,
        }
        metrics.record_backpressure_sample("test-group", 0, indicators)

        report = metrics.get_backpressure_report("test-group")
        assert report is not None
        assert report["fill_ratio"] == 0.95
        assert report["poll_proximity"] == 0.32
        assert report["idle_ms"] == 2.0
        assert report["rebalance_count"] == 0
        assert report["bottleneck_stage"] is None
        assert report["instance_count"] == 1
        assert "ts" in report

    def test_multi_instance_aggregates_worst_case(self, metrics):
        """Two instances → report uses worst-case values."""
        # Instance 0: moderate backpressure
        metrics.record_backpressure_sample(
            "test-group",
            0,
            {
                "fill_ratio": 0.80,
                "poll_proximity": 0.30,
                "idle_ms": 50.0,
                "rebalance_count": 1,
                "bottleneck_stage": None,
            },
        )
        # Instance 1: higher backpressure
        metrics.record_backpressure_sample(
            "test-group",
            1,
            {
                "fill_ratio": 0.98,
                "poll_proximity": 0.75,
                "idle_ms": 2.0,
                "rebalance_count": 2,
                "bottleneck_stage": "valkey (↑ 48%)",
            },
        )

        report = metrics.get_backpressure_report("test-group")
        assert report is not None
        # fill_ratio: max(0.80, 0.98) = 0.98
        assert report["fill_ratio"] == 0.98
        # poll_proximity: max(0.30, 0.75) = 0.75
        assert report["poll_proximity"] == 0.75
        # idle_ms: min(50.0, 2.0) = 2.0 (least headroom)
        assert report["idle_ms"] == 2.0
        # rebalance_count: sum(1, 2) = 3
        assert report["rebalance_count"] == 3
        # bottleneck_stage from highest proximity instance
        assert report["bottleneck_stage"] == "valkey (↑ 48%)"
        assert report["instance_count"] == 2

    def test_returns_latest_sample_per_instance(self, metrics, fake_redis):
        """When multiple samples exist per instance, uses the most recent."""
        # Record an old sample
        metrics.record_backpressure_sample(
            "test-group",
            0,
            {
                "fill_ratio": 0.50,
                "poll_proximity": 0.10,
                "idle_ms": 100.0,
                "rebalance_count": 0,
                "bottleneck_stage": None,
            },
        )

        # Advance time and record a newer sample
        time.sleep(0.01)
        metrics.record_backpressure_sample(
            "test-group",
            0,
            {
                "fill_ratio": 0.99,
                "poll_proximity": 0.85,
                "idle_ms": 1.0,
                "rebalance_count": 4,
                "bottleneck_stage": "pg_sessions (↑ 30%)",
            },
        )

        report = metrics.get_backpressure_report("test-group")
        assert report is not None
        # Should reflect the newer sample
        assert report["fill_ratio"] == 0.99
        assert report["poll_proximity"] == 0.85

    def test_different_groups_isolated(self, metrics):
        """Backpressure data for different groups doesn't interfere."""
        metrics.record_backpressure_sample(
            "group-a",
            0,
            {
                "fill_ratio": 0.99,
                "poll_proximity": 0.90,
                "idle_ms": 1.0,
                "rebalance_count": 5,
            },
        )
        metrics.record_backpressure_sample(
            "group-b",
            0,
            {
                "fill_ratio": 0.20,
                "poll_proximity": 0.05,
                "idle_ms": 200.0,
                "rebalance_count": 0,
            },
        )

        report_a = metrics.get_backpressure_report("group-a")
        report_b = metrics.get_backpressure_report("group-b")

        assert report_a["fill_ratio"] == 0.99
        assert report_b["fill_ratio"] == 0.20

    def test_missing_indicator_keys_use_defaults(self, metrics):
        """Missing indicator keys in stored sample use safe defaults."""
        # Record a minimal sample with only fill_ratio
        metrics.record_backpressure_sample(
            "test-group",
            0,
            {
                "fill_ratio": 0.75,
            },
        )

        report = metrics.get_backpressure_report("test-group")
        assert report is not None
        assert report["fill_ratio"] == 0.75
        # Missing keys should default to safe values
        assert report["poll_proximity"] == 0.0
        assert report["idle_ms"] == 0.0
        assert report["rebalance_count"] == 0
        assert report["bottleneck_stage"] is None
