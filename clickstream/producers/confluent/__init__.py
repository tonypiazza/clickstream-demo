# ==============================================================================
# Confluent Kafka Producer
# ==============================================================================
"""
Confluent Kafka producer implementation.

Uses the confluent-kafka library (librdkafka C wrapper) for high-performance
event publishing to Kafka.
"""

from clickstream.producers.confluent.producer import ConfluentProducer

__all__ = ["ConfluentProducer"]
