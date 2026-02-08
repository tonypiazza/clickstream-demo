# ==============================================================================
# Token Bucket Rate Limiter
# ==============================================================================
"""
Token bucket rate limiter for controlling event production throughput.

Provides a configurable sustained events/sec rate using the classic token
bucket algorithm.  Tokens accumulate at a fixed rate up to a burst cap;
each ``acquire()`` call consumes tokens, blocking when the bucket is empty.

Usage::

    limiter = TokenBucketRateLimiter(rate=5000)
    for event in events:
        limiter.acquire()          # blocks until a token is available
        producer.produce(event)
"""

import time
from collections.abc import Callable


class TokenBucketRateLimiter:
    """Rate limiter using the token bucket algorithm.

    Args:
        rate: Target events per second.
        burst: Maximum burst size (tokens the bucket can hold).
            Defaults to ``max(int(rate * 0.1), 100)``.
        shutdown_check: Optional callable that returns ``True`` when the
            producer has been asked to shut down.  Checked during waits
            so the limiter can exit promptly.
    """

    def __init__(
        self,
        rate: float,
        burst: int | None = None,
        shutdown_check: Callable[[], bool] | None = None,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}")

        self.rate = rate
        self.burst = burst if burst is not None else max(int(rate * 0.1), 100)
        self._shutdown_check = shutdown_check

        # Start with a full bucket so the first burst goes through immediately.
        self._tokens: float = float(self.burst)
        self._last_refill: float = time.monotonic()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self, count: int = 1) -> float:
        """Block until *count* tokens are available, then consume them.

        Args:
            count: Number of tokens to consume (default 1).

        Returns:
            Total seconds spent waiting (0.0 if tokens were available
            immediately).
        """
        self._refill()

        waited = 0.0

        while self._tokens < count:
            # Check for shutdown before sleeping
            if self._shutdown_check and self._shutdown_check():
                return waited

            # Calculate how long until enough tokens accumulate
            deficit = count - self._tokens
            sleep_time = min(deficit / self.rate, 0.01)
            time.sleep(sleep_time)
            waited += sleep_time
            self._refill()

        self._tokens -= count
        return waited

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        """Add tokens based on elapsed time since the last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._tokens + elapsed * self.rate, float(self.burst))
