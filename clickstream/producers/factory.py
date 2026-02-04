# ==============================================================================
# Producer Factory
# ==============================================================================
"""
Factory function for getting the active producer implementation.

The producer is selected based on the PRODUCER_IMPL environment variable.
"""

from clickstream.producers.base import StreamingProducer


def get_producer() -> StreamingProducer:
    """
    Get the producer instance based on PRODUCER_IMPL setting.

    The producer is determined by the PRODUCER_IMPL environment variable:
    - "confluent" (default): Uses confluent-kafka (librdkafka C library) - fastest
    - "kafka_python": Uses kafka-python library (pure Python)
    - "quix": Uses Quix Streams producer API

    Returns:
        StreamingProducer: The producer instance

    Raises:
        ImportError: If the selected producer package is not installed
        ValueError: If an unknown producer is specified

    Example:
        >>> from clickstream.producers import get_producer
        >>> producer = get_producer()
        >>> print(producer.name)
        'confluent-kafka'
    """
    from clickstream.utils.config import get_settings

    impl = get_settings().producer.impl

    match impl:
        case "confluent":
            from clickstream.producers.confluent import ConfluentProducer

            return ConfluentProducer()
        case "kafka_python":
            from clickstream.producers.kafka_python import KafkaPythonProducer

            return KafkaPythonProducer()
        case "quix":
            try:
                from clickstream.producers.quix import QuixProducer

                return QuixProducer()
            except ImportError as e:
                raise ImportError(
                    "Quix producer selected but 'quixstreams' not installed.\n"
                    "Install with: pip install -e '.[quix]'"
                ) from e
        case _:
            raise ValueError(
                f"Unknown producer: '{impl}'.\nValid options are: confluent, kafka_python, quix"
            )
