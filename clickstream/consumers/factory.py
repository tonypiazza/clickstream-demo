# ==============================================================================
# Consumer Factory
# ==============================================================================
"""
Factory function for creating consumer instances.

Uses CONSUMER_IMPL environment variable (via config) to determine
which implementation to use.
"""

from clickstream.consumers.base import ConsumerType, StreamingConsumer


def get_consumer(consumer_type: ConsumerType) -> StreamingConsumer:
    """
    Get a consumer instance based on configuration.

    The consumer is determined by the CONSUMER_IMPL environment variable:
    - "confluent" (default): Uses confluent-kafka (librdkafka C library) - fastest
    - "kafka_python": Uses kafka-python library (pure Python)
    - "quix": Uses Quix Streams consumer API
    - "mage": Uses Mage AI streaming pipelines
    - "bytewax": Uses Bytewax dataflow engine

    Args:
        consumer_type: Type of consumer ("postgresql" or "opensearch")

    Returns:
        StreamingConsumer instance for the configured implementation

    Raises:
        ValueError: If unknown implementation is configured
    """
    from clickstream.utils.config import get_settings

    impl = get_settings().consumer.impl

    match impl:
        case "confluent":
            from clickstream.consumers.confluent import ConfluentConsumer

            return ConfluentConsumer(consumer_type)
        case "kafka_python":
            from clickstream.consumers.kafka_python import KafkaPythonConsumer

            return KafkaPythonConsumer(consumer_type)
        case "quix":
            from clickstream.consumers.quix import QuixConsumer

            return QuixConsumer(consumer_type)
        case "mage":
            from clickstream.consumers.mage import MageConsumer

            return MageConsumer(consumer_type)
        case "bytewax":
            from clickstream.consumers.bytewax import BytewaxConsumer

            return BytewaxConsumer(consumer_type)
        case _:
            raise ValueError(
                f"Unknown consumer implementation: '{impl}'.\n"
                "Valid options are: confluent, kafka_python, quix, mage, bytewax"
            )
