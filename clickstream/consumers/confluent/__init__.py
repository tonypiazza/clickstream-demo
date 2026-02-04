# ==============================================================================
# Confluent Kafka Consumer
# ==============================================================================
"""
Confluent Kafka consumer implementation.

Uses the confluent-kafka library (librdkafka C wrapper) for high-performance
event consumption from Kafka.
"""

from clickstream.consumers.confluent.consumer import ConfluentConsumer

__all__ = ["ConfluentConsumer"]
