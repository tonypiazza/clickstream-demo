"""
Consumer module for clickstream event processing.

This module provides a unified interface for consuming events from Kafka
using different streaming framework implementations.

Supported implementations:
- kafka_python: Raw kafka-python (lightweight baseline)
- quix: Quix Streams (recommended for production)
- mage: Mage AI streaming pipelines
- bytewax: Bytewax (Python streaming with Rust performance)

Usage:
    from clickstream.consumers import get_consumer

    # Get a PostgreSQL consumer using the configured implementation
    consumer = get_consumer("postgresql")
    consumer.run()

    # Get an OpenSearch consumer
    consumer = get_consumer("opensearch")
    consumer.run()

Configuration:
    Set CONSUMER_IMPL environment variable to choose implementation:
    - CONSUMER_IMPL=kafka_python (default)
    - CONSUMER_IMPL=quix
    - CONSUMER_IMPL=mage
    - CONSUMER_IMPL=bytewax
"""

from clickstream.consumers.base import ConsumerType, StreamingConsumer
from clickstream.consumers.factory import get_consumer

__all__ = ["StreamingConsumer", "ConsumerType", "get_consumer"]
