#!/usr/bin/env python3
"""
Producer runner script - produces clickstream events to Kafka.

This script is started as a background process by 'clickstream producer start'.
It reads events from the CSV file and publishes them to Kafka with optional
real-time delays to simulate live traffic.
"""

import logging
import os
import signal
import sys
import time
import warnings

# Suppress noisy warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from clickstream.utils.paths import PRODUCER_LOG_FILE, get_project_root

# Configure logging to only write to file (not console)
LOG_FILE = os.environ.get("CLICKSTREAM_LOG_FILE", str(PRODUCER_LOG_FILE))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
    ],
)

# Suppress noisy third-party loggers
logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class GracefulShutdown(Exception):
    """Exception raised to trigger graceful shutdown."""

    pass


def signal_handler(signum, frame):
    """Handle shutdown signals by raising exception."""
    logger.info("Received signal %d, shutting down gracefully...", signum)
    raise GracefulShutdown()


def main():
    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Find project root
    project_root = get_project_root()

    # Set up environment
    python_path = os.environ.get("PYTHONPATH", "")
    if str(project_root) not in python_path:
        os.environ["PYTHONPATH"] = (
            f"{project_root}:{python_path}" if python_path else str(project_root)
        )

    # Get options from environment
    # PRODUCER_SPEED presence implies realtime mode; absence means batch mode
    speed_env = os.environ.get("PRODUCER_SPEED")
    realtime_mode = speed_env is not None
    speed = float(speed_env) if speed_env else 1.0
    limit_env = os.environ.get("PRODUCER_LIMIT")
    limit = int(limit_env) if limit_env else None
    rate_env = os.environ.get("PRODUCER_RATE")
    rate = float(rate_env) if rate_env else None

    # Get producer
    from clickstream.producers import get_producer
    from clickstream.utils.session_state import get_producer_messages
    from clickstream.utils.versions import get_clickstream_version

    producer = get_producer()

    logger.info(
        "Producer started | %s | clickstream-demo v%s",
        producer.name,
        get_clickstream_version(),
    )

    start_time = time.time()
    try:
        producer.run(limit=limit, realtime=realtime_mode, speed=speed, rate=rate)

    except GracefulShutdown:
        logger.info("Producer interrupted by shutdown signal.")
    except KeyboardInterrupt:
        logger.info("Producer interrupted by keyboard.")
    except Exception as e:
        logger.exception("Producer error: %s", e)
        sys.exit(1)

    # Log producer duration and throughput
    duration = time.time() - start_time
    events_sent = get_producer_messages() or 0
    throughput = round(events_sent / duration) if duration > 0 else 0
    logger.info(
        "Producer completed | %s | %d events | %.1f seconds | %d events/sec",
        producer.name,
        events_sent,
        duration,
        throughput,
    )

    logger.info("Producer shutdown complete.")


if __name__ == "__main__":
    main()
