#!/usr/bin/env python3
"""
Consumer runner script - runs streaming consumers for PostgreSQL or OpenSearch.

This script is started as a background process by 'clickstream consumer start'.
Supports multiple instances via --instance argument for parallel consumption.

Usage:
    python consumer_runner.py --type postgresql --instance 0
    python consumer_runner.py --type opensearch --instance 0
"""

import argparse
import logging
import os
import signal
import sys
import warnings

# Suppress noisy warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Parse arguments before configuring logging
parser = argparse.ArgumentParser(description="Clickstream consumer runner")
parser.add_argument(
    "--type",
    choices=["postgresql", "opensearch"],
    default="postgresql",
    help="Consumer type (postgresql or opensearch)",
)
parser.add_argument(
    "--instance", type=int, default=0, help="Consumer instance number (for parallel consumers)"
)
args = parser.parse_args()

from clickstream.utils.paths import get_consumer_log_file, get_project_root

# Configure logging to only write to file (not console)
# Log file path is based on consumer type and instance number
DEFAULT_LOG_FILE = str(get_consumer_log_file(args.instance, args.type))

# Environment variable for log file override (type-specific)
LOG_ENV_VAR = (
    "CLICKSTREAM_OPENSEARCH_LOG_FILE" if args.type == "opensearch" else "CLICKSTREAM_LOG_FILE"
)
LOG_FILE = os.environ.get(LOG_ENV_VAR, DEFAULT_LOG_FILE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
    ],
)

# Suppress noisy third-party loggers
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("quixstreams").setLevel(logging.INFO)
if args.type == "opensearch":
    logging.getLogger("opensearch").setLevel(logging.ERROR)

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

    # Set consumer group ID for last message timestamp tracking
    from clickstream.utils.config import get_settings

    settings = get_settings()

    if args.type == "postgresql":
        os.environ["CLICKSTREAM_CONSUMER_GROUP"] = settings.postgresql_consumer.group_id
    else:
        os.environ["CLICKSTREAM_CONSUMER_GROUP"] = settings.opensearch.consumer_group_id

    # Get consumer
    from clickstream.consumers import get_consumer
    from clickstream.utils.versions import get_clickstream_version

    consumer = get_consumer(args.type)

    consumer_name = "PostgreSQL" if args.type == "postgresql" else "OpenSearch"
    logger.info(
        "%s consumer started | %s | clickstream-demo v%s",
        consumer_name,
        consumer.version,
        get_clickstream_version(),
    )
    logger.info("Instance: %d", args.instance)

    try:
        consumer.run()

    except GracefulShutdown:
        logger.info("%s consumer interrupted by shutdown signal.", consumer_name)
    except KeyboardInterrupt:
        logger.info("%s consumer interrupted by keyboard.", consumer_name)
    except Exception as e:
        logger.exception("%s consumer error: %s", consumer_name, e)
        sys.exit(1)

    logger.info("%s consumer shutdown complete.", consumer_name)


if __name__ == "__main__":
    main()
