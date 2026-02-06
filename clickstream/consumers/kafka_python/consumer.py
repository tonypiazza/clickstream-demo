# ==============================================================================
# kafka-python Consumer
# ==============================================================================
"""
kafka-python consumer implementation.

Uses kafka-python for consuming with manual offset commits.
Leverages shared infrastructure repositories for persistence.
"""

from clickstream.consumers.base import StreamingConsumer
from clickstream.utils.versions import get_package_version


class KafkaPythonConsumer(StreamingConsumer):
    """
    kafka-python consumer implementation.

    Uses kafka-python for consuming with manual offset commits.
    Leverages shared infrastructure repositories for persistence.
    """

    @property
    def name(self) -> str:
        return "kafka-python"

    @property
    def version(self) -> str:
        return f"kafka-python v{get_package_version('kafka-python')}"

    def run(self) -> None:
        """Run the kafka-python consumer."""
        if self._consumer_type == "postgresql":
            from clickstream.consumers.kafka_python.postgresql_consumer import run

            run()
        elif self._consumer_type == "opensearch":
            from clickstream.consumers.kafka_python.opensearch_consumer import run

            run()
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")
