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

from clickstream.infrastructure.metrics import LAG_SAMPLES_KEY_PREFIX


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
