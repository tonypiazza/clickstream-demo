# ==============================================================================
# Shared Test Fixtures
# ==============================================================================
"""
Pytest fixtures shared across all test modules.

Provides:
- fakeredis-backed ValkeyCache and PipelineMetrics instances
- Clean Redis state per test (automatic flush)
"""

import fakeredis
import pytest

from clickstream.infrastructure.cache import ValkeyCache
from clickstream.infrastructure.metrics import PipelineMetrics


@pytest.fixture()
def fake_redis():
    """A clean fakeredis instance for each test.

    Uses decode_responses=True to match the real ValkeyCache behavior.
    """
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    yield client
    client.flushall()
    client.close()


@pytest.fixture()
def fake_cache(fake_redis):
    """A ValkeyCache with its internal client replaced by fakeredis.

    This avoids needing a real Valkey/Redis server for unit tests while
    exercising the full ValkeyCache API surface.
    """
    # Create a ValkeyCache with a dummy URL (won't actually connect)
    # then swap in the fake client
    cache = ValkeyCache.__new__(ValkeyCache)
    cache._client = fake_redis
    cache._url = "redis://fake:6379"
    return cache


@pytest.fixture()
def metrics(fake_cache):
    """A PipelineMetrics instance backed by fakeredis.

    Ready for testing lag recording, history retrieval, and trend analysis.
    """
    return PipelineMetrics(cache=fake_cache)
