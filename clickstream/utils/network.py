# ==============================================================================
# Network Utilities
# ==============================================================================
"""
Network utilities for measuring latency and bandwidth.

Used by the benchmark command to capture network characteristics
that affect pipeline throughput.
"""

import socket
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clickstream.utils.config import Settings


@dataclass
class NetworkMetrics:
    """Network measurement results."""

    kafka_latency_ms: float | None = None
    pg_latency_ms: float | None = None
    valkey_latency_ms: float | None = None
    upload_mbps: float | None = None
    ping_ms: float | None = None


def measure_tcp_latency(host: str, port: int, timeout: float = 5.0) -> float | None:
    """
    Measure TCP connection latency to a host.

    This measures the time to establish a TCP connection, which is a good
    proxy for network round-trip time to the service.

    Args:
        host: Hostname or IP address (can include port, e.g., "host:9092")
        port: Default port if not specified in host
        timeout: Connection timeout in seconds

    Returns:
        Latency in milliseconds, or None if connection failed
    """
    try:
        # Parse host if it contains multiple servers (e.g., "kafka1:9092,kafka2:9092")
        if "," in host:
            host = host.split(",")[0]  # Use first server

        # Parse host if it contains port (e.g., "kafka:9092")
        if ":" in host:
            host, port_str = host.rsplit(":", 1)
            port = int(port_str)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        start = time.perf_counter()
        sock.connect((host, port))
        latency = (time.perf_counter() - start) * 1000  # Convert to ms
        sock.close()

        return round(latency, 1)
    except Exception:
        return None


def measure_speedtest() -> tuple[float | None, float | None]:
    """
    Run speedtest to measure upload speed and ping.

    Uses speedtest.net infrastructure to measure general internet
    upload bandwidth and latency to the nearest speedtest server.

    Note: This can take 30-60 seconds to complete.

    Returns:
        Tuple of (upload_mbps, ping_ms), either can be None on failure
    """
    try:
        import speedtest

        s = speedtest.Speedtest(timeout=15, secure=True)
        s.get_best_server()
        s.upload(pre_allocate=True)

        upload_mbps = round(s.results.upload / 1_000_000, 1)
        ping_ms = round(s.results.ping, 1)

        return upload_mbps, ping_ms
    except Exception:
        return None, None


def measure_latencies(settings: "Settings") -> NetworkMetrics:
    """
    Measure TCP connection latencies to all configured services.

    This is fast (~1-2 seconds) and measures actual network latency
    to the services used by the pipeline.

    Args:
        settings: Application Settings object

    Returns:
        NetworkMetrics with latency measurements only
    """
    metrics = NetworkMetrics()

    # Measure Kafka latency (default port 9092)
    metrics.kafka_latency_ms = measure_tcp_latency(settings.kafka.bootstrap_servers, 9092)

    # Measure PostgreSQL latency
    metrics.pg_latency_ms = measure_tcp_latency(settings.postgres.host, settings.postgres.port)

    # Measure Valkey latency
    metrics.valkey_latency_ms = measure_tcp_latency(settings.valkey.host, settings.valkey.port)

    return metrics


def collect_network_metrics(settings: "Settings") -> NetworkMetrics:
    """
    Collect all network metrics including speedtest.

    This includes service latencies plus general internet bandwidth
    measurements via speedtest.net.

    Note: This can take 30-60 seconds due to the speedtest.

    Args:
        settings: Application Settings object

    Returns:
        NetworkMetrics with all measurements
    """
    # Start with latency measurements
    metrics = measure_latencies(settings)

    # Add speedtest measurements (upload + ping)
    metrics.upload_mbps, metrics.ping_ms = measure_speedtest()

    return metrics
