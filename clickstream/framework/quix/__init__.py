# ==============================================================================
# Quix Streams Framework Implementation
# ==============================================================================
"""
Quix Streams framework implementation.

Uses Quix Streams for Kafka consumption with custom BatchingSink adapters
that wrap the shared infrastructure layer (repositories).
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


class QuixProducer(StreamingProducer):
    """
    Quix Streams producer implementation.

    Uses Quix Streams' producer API for publishing events to Kafka.
    """

    @property
    def version(self) -> str:
        return f"Quix Streams v{get_package_version('quixstreams')}"

    def run(
        self,
        limit: Optional[int] = None,
        realtime: bool = False,
        speed: float = 1.0,
    ) -> None:
        """Run the Quix producer."""
        from clickstream.framework.quix.producer import run_producer

        run_producer(limit=limit, realtime=realtime, speed=speed)


class QuixConsumer(StreamingConsumer):
    """
    Quix Streams consumer implementation.

    Uses Quix Streams' BatchingSink API for efficient batch processing.
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
        return f"Quix Streams v{get_package_version('quixstreams')}"

    def run(self) -> None:
        """Run the Quix consumer."""
        if self._consumer_type == "postgresql":
            from clickstream.framework.quix.postgresql_consumer import run

            run()
        elif self._consumer_type == "opensearch":
            from clickstream.framework.quix.opensearch_consumer import run

            run()
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")


class QuixFramework(StreamingFramework):
    """
    Quix Streams framework adapter.

    Provides consumer and producer implementations using Quix Streams library.
    Consumers use Quix's BatchingSink API for efficient batch processing.
    """

    @property
    def name(self) -> str:
        return "Quix Streams"

    @property
    def project_path(self) -> Path:
        return get_project_root() / "clickstream" / "framework" / "quix"

    def get_producer(self) -> StreamingProducer:
        """Get the Quix producer instance."""
        return QuixProducer()

    def get_consumer(self, consumer_type: str) -> StreamingConsumer:
        """Get a Quix consumer instance."""
        return QuixConsumer(consumer_type)


# Re-export commonly used items for convenience
from clickstream.framework.quix.config import (
    create_application,
    create_topic,
    ensure_topic_exists,
)
from clickstream.framework.quix.sinks import (
    OpenSearchEventSink,
    PostgreSQLEventSink,
    PostgreSQLSessionSink,
)

__all__ = [
    "QuixFramework",
    "QuixProducer",
    "QuixConsumer",
    "create_application",
    "create_topic",
    "ensure_topic_exists",
    "OpenSearchEventSink",
    "PostgreSQLEventSink",
    "PostgreSQLSessionSink",
]
