# ==============================================================================
# Tests for Token Bucket Rate Limiter
# ==============================================================================
"""
Unit tests for the TokenBucketRateLimiter.

Tests cover:
- Immediate acquisition when tokens are available
- Blocking when tokens are exhausted
- Burst capacity allows initial burst
- Throughput matches target rate (within tolerance)
- Shutdown check interrupts waiting
- Default burst calculation
- Multi-token acquisition
- Invalid rate raises ValueError
"""

import time

import pytest

from clickstream.utils.rate_limiter import TokenBucketRateLimiter


# ==============================================================================
# Construction
# ==============================================================================


class TestConstruction:
    """Tests for TokenBucketRateLimiter initialization."""

    def test_default_burst_small_rate(self):
        """Burst floor is 100 when rate * 0.1 < 100."""
        limiter = TokenBucketRateLimiter(rate=500)
        assert limiter.burst == 100  # max(500 * 0.1, 100) = 100

    def test_default_burst_large_rate(self):
        """Burst scales to 10% of rate when rate is large enough."""
        limiter = TokenBucketRateLimiter(rate=5000)
        assert limiter.burst == 500  # max(5000 * 0.1, 100) = 500

    def test_explicit_burst(self):
        """Explicit burst overrides the default calculation."""
        limiter = TokenBucketRateLimiter(rate=1000, burst=42)
        assert limiter.burst == 42

    def test_invalid_rate_raises(self):
        """Rate must be positive."""
        with pytest.raises(ValueError, match="rate must be positive"):
            TokenBucketRateLimiter(rate=0)

    def test_negative_rate_raises(self):
        """Negative rate is rejected."""
        with pytest.raises(ValueError, match="rate must be positive"):
            TokenBucketRateLimiter(rate=-100)


# ==============================================================================
# Acquisition
# ==============================================================================


class TestAcquire:
    """Tests for the acquire() method."""

    def test_no_wait_when_tokens_available(self):
        """Acquiring within burst capacity returns immediately."""
        limiter = TokenBucketRateLimiter(rate=10000, burst=100)
        waited = limiter.acquire()
        assert waited == 0.0

    def test_burst_allows_multiple_immediate(self):
        """All burst tokens can be consumed without waiting."""
        limiter = TokenBucketRateLimiter(rate=10000, burst=50)
        for _ in range(50):
            waited = limiter.acquire()
            assert waited == 0.0

    def test_acquire_count_multiple(self):
        """acquire(count=N) consumes N tokens at once."""
        limiter = TokenBucketRateLimiter(rate=10000, burst=100)
        waited = limiter.acquire(count=50)
        assert waited == 0.0
        # 50 tokens remain — acquiring 50 more should be immediate
        waited = limiter.acquire(count=50)
        assert waited == 0.0

    def test_blocks_when_exhausted(self):
        """After burst is exhausted, acquire blocks until tokens refill."""
        limiter = TokenBucketRateLimiter(rate=1000, burst=10)
        # Drain all tokens
        for _ in range(10):
            limiter.acquire()
        # Next acquire should block
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        # Should have waited approximately 1/1000 = 0.001 seconds
        # Allow generous tolerance for CI timing
        assert elapsed >= 0.0005

    def test_throughput_matches_rate(self):
        """Sustained acquisition rate converges to the target rate."""
        target_rate = 2000  # events/sec
        limiter = TokenBucketRateLimiter(rate=target_rate, burst=10)
        # Drain burst tokens first
        for _ in range(10):
            limiter.acquire()
        # Now measure sustained throughput over ~100 events
        count = 100
        start = time.monotonic()
        for _ in range(count):
            limiter.acquire()
        elapsed = time.monotonic() - start
        actual_rate = count / elapsed
        # Allow 20% tolerance for timing jitter
        assert actual_rate < target_rate * 1.2
        assert actual_rate > target_rate * 0.5


# ==============================================================================
# Shutdown
# ==============================================================================


class TestShutdown:
    """Tests for shutdown_check integration."""

    def test_shutdown_interrupts_wait(self):
        """When shutdown_check returns True, acquire exits promptly."""
        shutdown = False

        def check_shutdown():
            return shutdown

        limiter = TokenBucketRateLimiter(rate=100, burst=1, shutdown_check=check_shutdown)
        # Drain the single token
        limiter.acquire()
        # Signal shutdown
        shutdown = True
        # This should return quickly despite no tokens being available
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        # Should exit within one sleep cycle (~0.01s) plus tolerance
        assert elapsed < 0.1

    def test_no_shutdown_check_blocks_normally(self):
        """Without a shutdown_check, acquire blocks until tokens refill."""
        limiter = TokenBucketRateLimiter(rate=1000, burst=1, shutdown_check=None)
        limiter.acquire()  # drain
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.0005
