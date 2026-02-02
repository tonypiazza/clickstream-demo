# ==============================================================================
# Bytewax Framework Implementation
# ==============================================================================
"""
Bytewax streaming framework implementation.

Uses Bytewax dataflows for Kafka consumption with custom sinks
that wrap the shared infrastructure layer (repositories).

For the producer, Bytewax delegates to the Default framework's producer
(kafka-python) since Bytewax is optimized for stream processing rather
than batch production.

Bytewax is a Python-native stream processing framework with a Rust-based
distributed engine, providing high performance with a Pythonic API.
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


class BytewaxProducer(StreamingProducer):
    """
    Bytewax producer implementation.

    Delegates to DefaultProducer (kafka-python) for publishing events to Kafka.
    This is more efficient than Bytewax's dataflow-based producer for batch loading,
    as Bytewax is optimized for stream processing rather than batch production.
    """

    @property
    def version(self) -> str:
        # Delegate to DefaultProducer for version
        from clickstream.framework.default import DefaultProducer

        return DefaultProducer().version

    def run(
        self,
        limit: Optional[int] = None,
        realtime: bool = False,
        speed: float = 1.0,
    ) -> None:
        """Run the producer by delegating to DefaultProducer."""
        from clickstream.framework.default import DefaultProducer

        DefaultProducer().run(limit=limit, realtime=realtime, speed=speed)


class BytewaxConsumer(StreamingConsumer):
    """
    Bytewax consumer implementation.

    Uses Bytewax's dataflow API with custom sinks for efficient batch processing.
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
        return f"Bytewax v{get_package_version('bytewax')}"

    def run(self) -> None:
        """Run the Bytewax consumer."""
        if self._consumer_type == "postgresql":
            from clickstream.framework.bytewax.postgresql_consumer import run

            run()
        elif self._consumer_type == "opensearch":
            from clickstream.framework.bytewax.opensearch_consumer import run

            run()
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")


class BytewaxFramework(StreamingFramework):
    """
    Bytewax streaming framework adapter.

    Provides consumer implementations using Bytewax dataflows.
    Producer delegates to DefaultProducer (kafka-python) for better performance.
    Uses custom sinks that wrap the shared infrastructure repositories.
    """

    @property
    def name(self) -> str:
        return "Bytewax"

    @property
    def project_path(self) -> Path:
        return get_project_root() / "clickstream" / "framework" / "bytewax"

    def get_producer(self) -> StreamingProducer:
        """Get the Bytewax producer instance."""
        return BytewaxProducer()

    def get_consumer(self, consumer_type: str) -> StreamingConsumer:
        """Get a Bytewax consumer instance."""
        return BytewaxConsumer(consumer_type)


__all__ = ["BytewaxFramework", "BytewaxProducer", "BytewaxConsumer"]
