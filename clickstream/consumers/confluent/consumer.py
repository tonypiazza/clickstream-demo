# ==============================================================================
# Confluent Kafka Consumer
# ==============================================================================
"""
Consumer implementation using confluent-kafka (librdkafka C wrapper).

This is the highest-performance consumer option, using the native C library
for Kafka protocol handling. Significantly faster than pure-Python kafka-python.
"""

from clickstream.consumers.base import StreamingConsumer
from clickstream.utils.versions import get_package_version


class ConfluentConsumer(StreamingConsumer):
    """
    Confluent-kafka consumer implementation.

    Uses the confluent-kafka library (librdkafka C wrapper) for high-performance
    event consumption from Kafka.
    """

    @property
    def name(self) -> str:
        return "confluent-kafka"

    @property
    def version(self) -> str:
        return f"confluent-kafka v{get_package_version('confluent-kafka')}"

    def run(self) -> None:
        """Run the confluent-kafka consumer."""
        if self._consumer_type == "postgresql":
            from clickstream.consumers.confluent.postgresql_consumer import run

            run()
        elif self._consumer_type == "opensearch":
            from clickstream.consumers.confluent.opensearch_consumer import run

            run()
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")
