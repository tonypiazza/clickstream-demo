# ==============================================================================
# Mage AI Framework Implementation
# ==============================================================================
"""
Mage AI framework implementation.

Uses Mage pipelines for Kafka consumption. For the producer, Mage delegates
to the Default framework's producer (kafka-python) since it provides better
performance than Mage's built-in producer.
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


class MageProducer(StreamingProducer):
    """
    Mage producer implementation.

    Delegates to DefaultProducer (kafka-python) for publishing events to Kafka.
    This is more efficient than Mage's built-in producer for this use case.
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


class MageConsumer(StreamingConsumer):
    """
    Mage consumer implementation.

    Uses Mage pipelines for streaming data processing.
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
        return f"Mage AI v{get_package_version('mage-ai')}"

    def run(self) -> None:
        """Run the Mage consumer pipeline."""
        from clickstream.framework.mage.executor import execute_streaming_pipeline

        if self._consumer_type == "postgresql":
            execute_streaming_pipeline("postgresql_consumer")
        elif self._consumer_type == "opensearch":
            execute_streaming_pipeline("opensearch_consumer")
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")


class MageFramework(StreamingFramework):
    """
    Mage AI framework adapter.

    Provides consumer implementations using Mage pipelines.
    Producer delegates to DefaultProducer (kafka-python) for better performance.
    Each pipeline is composed of data_loaders → transformers → data_exporters.
    """

    @property
    def name(self) -> str:
        return "Mage AI"

    @property
    def project_path(self) -> Path:
        return get_project_root() / "clickstream" / "framework" / "mage"

    def get_producer(self) -> StreamingProducer:
        """Get the Mage producer instance (delegates to DefaultProducer)."""
        return MageProducer()

    def get_consumer(self, consumer_type: str) -> StreamingConsumer:
        """Get a Mage consumer instance."""
        return MageConsumer(consumer_type)


__all__ = ["MageFramework", "MageProducer", "MageConsumer"]
