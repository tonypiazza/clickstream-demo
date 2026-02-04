# ==============================================================================
# Quix Streams Consumer
# ==============================================================================
"""
Quix Streams consumer implementation.

Uses Quix Streams' BatchingSink API for efficient batch processing.
"""

from clickstream.consumers.base import ConsumerType, StreamingConsumer
from clickstream.utils.versions import get_package_version


class QuixConsumer(StreamingConsumer):
    """
    Quix Streams consumer implementation.

    Uses Quix Streams' BatchingSink API for efficient batch processing.
    """

    @property
    def name(self) -> str:
        return "quixstreams"

    @property
    def version(self) -> str:
        return f"quixstreams v{get_package_version('quixstreams')}"

    def run(self) -> None:
        """Run the Quix consumer."""
        if self._consumer_type == "postgresql":
            from clickstream.consumers.quix.postgresql_consumer import run

            run()
        elif self._consumer_type == "opensearch":
            from clickstream.consumers.quix.opensearch_consumer import run

            run()
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")
