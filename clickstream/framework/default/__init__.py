# ==============================================================================
# Default Framework Implementation (kafka-python)
# ==============================================================================
"""
Default framework implementation using raw kafka-python.

This is the baseline streaming implementation with no external streaming
framework dependencies (no Quix Streams, no Mage AI). It uses:
- kafka-python for Kafka producer and consumer
- Shared infrastructure repositories for persistence
- Valkey for session state management

Other frameworks (like Mage) can delegate to this implementation when they
don't provide their own producer or consumer.
"""

from pathlib import Path
from typing import Optional

from clickstream.framework.base import (
    StreamingConsumer,
    StreamingFramework,
    StreamingProducer,
)
from clickstream.utils.paths import get_project_root
from clickstream.utils.versions import get_package_version


class DefaultProducer(StreamingProducer):
    """
    Default producer implementation using kafka-python.

    Uses kafka-python directly with Polars for fast CSV loading.
    This is the baseline producer that other frameworks can delegate to.
    """

    @property
    def version(self) -> str:
        return f"kafka-python v{get_package_version('kafka-python')}"

    def run(
        self,
        limit: Optional[int] = None,
        realtime: bool = False,
        speed: float = 1.0,
    ) -> None:
        """Run the kafka-python producer."""
        from clickstream.framework.default.producer import run_producer

        run_producer(limit=limit, realtime=realtime, speed=speed)


class DefaultConsumer(StreamingConsumer):
    """
    Default consumer implementation using kafka-python.

    Uses kafka-python for consuming with manual offset commits.
    Leverages shared infrastructure repositories for persistence.
    """

    def __init__(self, consumer_type: str):
        """
        Initialize the consumer.

        Args:
            consumer_type: Type of consumer ("postgresql" or "opensearch")
        """
        self._consumer_type = consumer_type

    @property
    def version(self) -> str:
        return f"kafka-python v{get_package_version('kafka-python')}"

    def run(self) -> None:
        """Run the kafka-python consumer."""
        if self._consumer_type == "postgresql":
            from clickstream.framework.default.postgresql_consumer import run

            run()
        elif self._consumer_type == "opensearch":
            from clickstream.framework.default.opensearch_consumer import run

            run()
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")


class DefaultFramework(StreamingFramework):
    """
    Default framework using raw kafka-python.

    This is the baseline implementation with no external streaming framework
    dependencies. It provides:
    - Producer using kafka-python + Polars for fast CSV loading
    - Consumers using kafka-python + shared infrastructure repositories

    Other frameworks can delegate to DefaultProducer or DefaultConsumer
    when they don't have their own implementation.
    """

    @property
    def name(self) -> str:
        return "kafka-python (Default)"

    @property
    def project_path(self) -> Path:
        return get_project_root() / "clickstream" / "framework" / "default"

    def get_producer(self) -> StreamingProducer:
        """Get the Default producer instance."""
        return DefaultProducer()

    def get_consumer(self, consumer_type: str) -> StreamingConsumer:
        """Get a Default consumer instance."""
        return DefaultConsumer(consumer_type)


__all__ = ["DefaultFramework", "DefaultProducer", "DefaultConsumer"]
