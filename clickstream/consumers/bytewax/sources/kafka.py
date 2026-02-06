# ==============================================================================
# Custom Kafka Source with Explicit Offset Commits
# ==============================================================================
"""
Custom Kafka source with explicit offset commits.

Bytewax's built-in KafkaSource uses consumer.assign() which bypasses Kafka's
consumer group coordination. The auto-commit setting has no effect with assign()
because auto-commit only works with subscribe().

This custom source adds explicit consumer.commit() calls after each batch to
ensure offsets are properly tracked in the consumer group.

Key differences from Bytewax's built-in KafkaSource:
1. Explicit synchronous commits after each batch
2. Simplified error handling (raise on error, no oks/errs split)
3. Commit failures raise exceptions immediately
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple, Union

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka.admin import AdminClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KafkaSourceMessage:
    """
    Message consumed from Kafka.

    Compatible with Bytewax's KafkaSourceMessage for downstream processing.
    """

    key: Optional[bytes]
    """Message key."""

    value: Optional[bytes]
    """Message value."""

    topic: str
    """Topic the message was consumed from."""

    headers: List[Tuple[str, bytes]] = field(default_factory=list)
    """Message headers."""

    latency: Optional[float] = None
    """Broker latency in seconds."""

    offset: int = 0
    """Message offset in partition."""

    partition: int = 0
    """Partition the message was consumed from."""

    timestamp: Tuple[int, int] = (0, 0)
    """Message timestamp as (type, value) tuple."""


def _list_parts(client: AdminClient, topics: Iterable[str]) -> Iterable[str]:
    """
    List all partitions for the given topics.

    Args:
        client: Kafka admin client
        topics: Topics to list partitions for

    Yields:
        Partition identifiers in format "{partition_idx}-{topic}"
    """
    for topic in topics:
        cluster_metadata = client.list_topics(topic)
        assert cluster_metadata.topics is not None
        topic_metadata = cluster_metadata.topics[topic]
        if topic_metadata.error is not None:
            msg = (
                f"Error listing partitions for Kafka topic `{topic!r}`: "
                f"{topic_metadata.error.str()}"
            )
            raise RuntimeError(msg)
        assert topic_metadata.partitions is not None
        part_idxs = topic_metadata.partitions.keys()
        for i in part_idxs:
            yield f"{i}-{topic}"


class _KafkaSourcePartitionWithCommit(StatefulSourcePartition[KafkaSourceMessage, Optional[int]]):
    """
    Partition handler that explicitly commits offsets after each batch.

    This ensures Kafka consumer group offsets are accurately tracked for
    monitoring purposes, even though Bytewax uses assign() instead of subscribe().
    """

    def __init__(
        self,
        step_id: str,
        config: Dict[str, str],
        topic: str,
        part_idx: int,
        starting_offset: int,
        resume_state: Optional[int],
        batch_size: int,
        poll_timeout: float,
    ):
        """
        Initialize the partition handler.

        Args:
            step_id: Unique step identifier for metrics
            config: Kafka consumer configuration
            topic: Topic to consume from
            part_idx: Partition index
            starting_offset: Initial offset (OFFSET_BEGINNING, OFFSET_END, etc.)
            resume_state: Offset to resume from (from Bytewax recovery)
            batch_size: Maximum messages to consume per batch
            poll_timeout: Timeout in seconds for each poll
        """
        self._offset = starting_offset if resume_state is None else resume_state
        self._consumer = Consumer(config)
        # assign() bypasses consumer group coordination but we commit explicitly
        self._consumer.assign([TopicPartition(topic, part_idx, self._offset)])
        self._topic = topic
        self._part_idx = part_idx
        self._batch_size = batch_size
        self._poll_timeout = poll_timeout
        self._eof = False
        self._step_id = step_id

        logger.debug(
            "Initialized Kafka partition %s-%d at offset %s",
            topic,
            part_idx,
            self._offset,
        )

    def next_batch(self) -> List[KafkaSourceMessage]:
        """
        Fetch next batch of messages and commit offsets.

        Returns:
            List of KafkaSourceMessage objects

        Raises:
            StopIteration: When end of partition is reached (non-tail mode)
            RuntimeError: On Kafka errors or commit failures
        """
        if self._eof:
            raise StopIteration()

        # Consume messages from Kafka
        msgs = self._consumer.consume(self._batch_size, timeout=self._poll_timeout)

        # Construct KafkaSourceMessage objects
        batch: List[KafkaSourceMessage] = []
        last_offset = None

        for msg in msgs:
            error = msg.error()
            if error is not None:
                if error.code() == ConfluentKafkaError._PARTITION_EOF:
                    # End of partition - set flag and process remaining messages
                    self._eof = True
                    break
                else:
                    # Raise on any other error
                    err_msg = (
                        f"Error consuming from Kafka topic `{self._topic!r}` "
                        f"partition {self._part_idx}: {error}"
                    )
                    raise RuntimeError(err_msg)

            headers = msg.headers()
            if headers is None:
                headers = []

            kafka_msg = KafkaSourceMessage(
                key=msg.key(),
                value=msg.value(),
                topic=msg.topic(),
                headers=headers,
                latency=msg.latency(),
                offset=msg.offset(),
                partition=msg.partition(),
                timestamp=msg.timestamp(),
            )
            batch.append(kafka_msg)
            last_offset = msg.offset()

        # Commit offsets
        if last_offset is not None:
            self._offset = last_offset + 1
            try:
                # Synchronous commit - raises on failure
                self._consumer.commit(
                    offsets=[TopicPartition(self._topic, self._part_idx, self._offset)],
                    asynchronous=False,
                )
            except Exception as e:
                logger.error(
                    "Failed to commit offset %d for %s-%d: %s",
                    self._offset,
                    self._topic,
                    self._part_idx,
                    e,
                )
                raise

        return batch

    def snapshot(self) -> Optional[int]:
        """
        Return current offset for Bytewax recovery system.

        Returns:
            Current offset position
        """
        return self._offset

    def close(self) -> None:
        """Close the Kafka consumer."""
        try:
            self._consumer.close()
        except Exception as e:
            logger.warning("Error closing Kafka consumer: %s", e)


class KafkaSourceWithCommit(FixedPartitionedSource[KafkaSourceMessage, Optional[int]]):
    """
    Kafka source that explicitly commits offsets.

    This source is similar to Bytewax's built-in KafkaSource but adds explicit
    offset commits after each batch. This ensures that Kafka consumer group
    offsets are properly tracked.

    Key features:
    - Explicit synchronous commits after each batch
    - Simplified error handling (raises on error)
    - Compatible with Bytewax's recovery system
    - Supports SSL configuration for Aiven deployments

    Usage:
        source = KafkaSourceWithCommit(
            brokers=["localhost:9092"],
            topics=["my-topic"],
            add_config={"group.id": "my-group"},
            batch_size=5000,
        )
        stream = op.input("kafka_in", flow, source)
    """

    def __init__(
        self,
        brokers: Iterable[str],
        topics: Iterable[str],
        tail: bool = True,
        starting_offset: int = OFFSET_BEGINNING,
        add_config: Optional[Dict[str, str]] = None,
        batch_size: int = 1000,
        poll_timeout: float = 0.1,
    ):
        """
        Initialize the Kafka source.

        Args:
            brokers: List of broker addresses (host:port)
            topics: List of topics to consume from
            tail: Whether to wait for new messages (True) or stop at end (False)
            starting_offset: Initial offset (OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED)
            add_config: Additional Kafka configuration (group.id, SSL settings, etc.)
            batch_size: Maximum messages to consume per batch
            poll_timeout: Timeout in seconds for each poll (default 0.1s / 100ms)
        """
        if isinstance(brokers, str):
            raise TypeError("brokers must be an iterable, not a string")
        if isinstance(topics, str):
            raise TypeError("topics must be an iterable, not a string")

        self._brokers = list(brokers)
        self._topics = list(topics)
        self._tail = tail
        self._starting_offset = starting_offset
        self._add_config = add_config or {}
        self._batch_size = batch_size
        self._poll_timeout = poll_timeout

    def list_parts(self) -> List[str]:
        """
        List all partitions for configured topics.

        Returns:
            List of partition identifiers ("{partition_idx}-{topic}")
        """
        config = {
            "bootstrap.servers": ",".join(self._brokers),
        }
        config.update(self._add_config)
        client = AdminClient(config)

        return list(_list_parts(client, self._topics))

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _KafkaSourcePartitionWithCommit:
        """
        Build a partition handler for the specified partition.

        Args:
            step_id: Unique step identifier
            for_part: Partition identifier ("{partition_idx}-{topic}")
            resume_state: Offset to resume from (from Bytewax recovery)

        Returns:
            Partition handler instance
        """
        idx, topic = for_part.split("-", 1)
        part_idx = int(idx)

        assert topic in self._topics, f"Can't resume from different set of Kafka topics"

        config = {
            # We use our own group.id from add_config
            "group.id": self._add_config.get("group.id", "bytewax-consumer"),
            # Disable auto-commit - we commit explicitly
            "enable.auto.commit": "false",
            "bootstrap.servers": ",".join(self._brokers),
            "enable.partition.eof": str(not self._tail).lower(),
        }
        config.update(self._add_config)
        # Ensure auto-commit is disabled even if add_config tried to enable it
        config["enable.auto.commit"] = "false"

        return _KafkaSourcePartitionWithCommit(
            step_id,
            config,
            topic,
            part_idx,
            self._starting_offset,
            resume_state,
            self._batch_size,
            self._poll_timeout,
        )
