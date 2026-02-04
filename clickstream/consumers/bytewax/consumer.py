# ==============================================================================
# Bytewax Consumer
# ==============================================================================
"""
Bytewax consumer implementation.

Uses Bytewax's dataflow API with custom sinks for efficient batch processing.
"""

from clickstream.consumers.base import ConsumerType, StreamingConsumer
from clickstream.utils.versions import get_package_version


class BytewaxConsumer(StreamingConsumer):
    """
    Bytewax consumer implementation.

    Uses Bytewax's dataflow API with custom sinks for efficient batch processing.
    """

    @property
    def name(self) -> str:
        return "bytewax"

    @property
    def version(self) -> str:
        return f"bytewax v{get_package_version('bytewax')}"

    def run(self) -> None:
        """Run the Bytewax consumer."""
        if self._consumer_type == "postgresql":
            from clickstream.consumers.bytewax.postgresql_consumer import run

            run()
        elif self._consumer_type == "opensearch":
            from clickstream.consumers.bytewax.opensearch_consumer import run

            run()
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")
