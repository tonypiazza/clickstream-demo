# ==============================================================================
# Mage AI Consumer
# ==============================================================================
"""
Mage AI consumer implementation.

Uses Mage pipelines for streaming data processing.

Note: Mage requires a specific project directory structure (pipelines/,
data_loaders/, transformers/, data_exporters/). The Mage project files
are located at clickstream/consumers/mage/ and must maintain this structure.
"""

from clickstream.consumers.base import ConsumerType, StreamingConsumer
from clickstream.utils.versions import get_package_version


class MageConsumer(StreamingConsumer):
    """
    Mage AI consumer implementation.

    Uses Mage pipelines for streaming data processing.
    """

    @property
    def name(self) -> str:
        return "mage-ai"

    @property
    def version(self) -> str:
        return f"mage-ai v{get_package_version('mage-ai')}"

    def run(self) -> None:
        """Run the Mage consumer pipeline."""
        from clickstream.consumers.mage.executor import execute_streaming_pipeline

        if self._consumer_type == "postgresql":
            execute_streaming_pipeline("postgresql_consumer")
        elif self._consumer_type == "opensearch":
            execute_streaming_pipeline("opensearch_consumer")
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")
