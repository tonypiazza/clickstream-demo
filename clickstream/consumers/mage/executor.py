# ==============================================================================
# Mage Pipeline Executor
# ==============================================================================
"""
Mage pipeline executor.

Provides execute_streaming_pipeline() to run Mage streaming pipelines
as custom streaming loops that invoke the pipeline blocks directly.

Note: Mage's built-in StreamingPipelineExecutor requires specific source
connectors (Kafka, Kinesis, etc.) configured via YAML. To support SSL
and advanced Kafka configurations from environment variables, we implement
a custom streaming loop that invokes block functions directly.
"""

import contextlib
import io
import json
import logging
import os
import signal
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Global flag for shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals by setting flag."""
    global _shutdown_requested
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown_requested = True


def get_mage_project_path() -> str:
    """Get the path to the Mage project directory."""
    from clickstream.utils.paths import get_project_root

    return str(get_project_root() / "clickstream" / "consumers" / "mage")


def _build_consumer_config(group_id: str) -> dict:
    """
    Build confluent-kafka consumer configuration.

    Args:
        group_id: Consumer group ID

    Returns:
        Dict with confluent-kafka consumer configuration
    """
    from clickstream.utils.config import get_settings
    from clickstream.utils.paths import get_project_root

    settings = get_settings()
    kafka_settings = settings.kafka
    consumer_settings = settings.consumer

    config = {
        "bootstrap.servers": kafka_settings.bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": consumer_settings.auto_offset_reset,
        "enable.auto.commit": False,
        "fetch.min.bytes": 1024,
        "fetch.max.bytes": 52428800,
        "max.partition.fetch.bytes": 1048576,
        "queued.min.messages": 10000,
        "queued.max.messages.kbytes": 65536,
        "session.timeout.ms": 45000,
        "heartbeat.interval.ms": 15000,
        "max.poll.interval.ms": 300000,
    }

    # Add SSL config if using SSL protocol
    if kafka_settings.security_protocol == "SSL":
        project_root = get_project_root()
        config["security.protocol"] = "SSL"

        if kafka_settings.ssl_ca_file:
            ca_path = project_root / kafka_settings.ssl_ca_file
            if ca_path.exists():
                config["ssl.ca.location"] = str(ca_path)

        if kafka_settings.ssl_cert_file:
            cert_path = project_root / kafka_settings.ssl_cert_file
            if cert_path.exists():
                config["ssl.certificate.location"] = str(cert_path)

        if kafka_settings.ssl_key_file:
            key_path = project_root / kafka_settings.ssl_key_file
            if key_path.exists():
                config["ssl.key.location"] = str(key_path)
    else:
        config["security.protocol"] = "PLAINTEXT"

    return config


def execute_streaming_pipeline(
    pipeline_name: str,
    variables: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Execute a Mage streaming pipeline using custom streaming loop.

    This implementation uses confluent-kafka directly for message consumption
    and invokes Mage transformer/exporter blocks for processing. This approach
    supports SSL authentication and advanced Kafka configurations.

    Args:
        pipeline_name: Name of the streaming pipeline (e.g., "postgresql_consumer")
        variables: Optional pipeline variables

    Raises:
        ImportError: If required packages are not installed
        RuntimeError: If pipeline not found or execution fails
    """
    global _shutdown_requested
    _shutdown_requested = False

    # Set up signal handlers
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    from confluent_kafka import Consumer, KafkaError
    from clickstream.utils.config import get_settings

    settings = get_settings()

    logger.info("Pipeline: %s", pipeline_name)

    if pipeline_name == "postgresql_consumer":
        _run_postgresql_consumer(settings)
    elif pipeline_name == "opensearch_consumer":
        _run_opensearch_consumer(settings)
    else:
        raise RuntimeError(f"Unknown pipeline: {pipeline_name}")


def _run_postgresql_consumer(settings) -> None:
    """Run the PostgreSQL consumer pipeline."""
    global _shutdown_requested

    from confluent_kafka import Consumer, KafkaError
    from clickstream.infrastructure.repositories.postgresql import (
        PostgreSQLEventRepository,
        PostgreSQLSessionRepository,
    )
    from clickstream.utils.session_state import (
        SessionState,
        get_valkey_client,
        set_last_message_timestamp,
    )

    group_id = settings.postgresql_consumer.group_id
    topic = settings.kafka.events_topic
    batch_size = settings.consumer.batch_size
    poll_timeout = settings.consumer.poll_timeout_ms / 1000.0

    # Build consumer config
    config = _build_consumer_config(group_id)
    consumer = Consumer(config)
    consumer.subscribe([topic])

    # Initialize repositories
    event_repo = PostgreSQLEventRepository(settings)
    event_repo.connect()

    session_repo = PostgreSQLSessionRepository(settings)
    session_repo.connect()

    # Initialize Valkey session state
    valkey_client = get_valkey_client()
    session_state = SessionState(
        valkey_client,
        timeout_minutes=settings.postgresql_consumer.session_timeout_minutes,
        ttl_hours=settings.valkey.session_ttl_hours,
    )

    logger.info("Starting PostgreSQL consumer (Mage AI)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", topic)

    try:
        while not _shutdown_requested:
            # Consume messages
            messages = consumer.consume(
                num_messages=batch_size,
                timeout=poll_timeout,
            )

            if not messages:
                continue

            # Parse messages
            events = []
            for msg in messages:
                if msg.error():
                    error = msg.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error("Consumer error: %s", error)
                        continue

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    events.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Failed to decode message: %s", e)
                    continue

            if not events:
                continue

            try:
                # 1. Save events to PostgreSQL
                event_repo.save(events)

                # 2. Update sessions in Valkey (batch operation)
                updated_sessions = session_state.batch_update_sessions(events)

                # 3. Convert and save sessions to PostgreSQL
                session_records = [session_state.to_db_record(s) for s in updated_sessions]
                session_repo.save(session_records)

                # 4. Track activity for status display
                set_last_message_timestamp(group_id)

                # 5. Commit offsets
                consumer.commit()

                logger.debug(
                    "Processed batch: %d events, %d sessions",
                    len(events),
                    len(session_records),
                )

            except Exception as e:
                logger.error("Error processing batch: %s", e)
                event_repo.rollback()
                session_repo.rollback()

                try:
                    event_repo.reconnect()
                    session_repo.reconnect()
                    logger.info("Reconnected to PostgreSQL after error")
                except Exception as reconnect_error:
                    logger.error("Failed to reconnect: %s", reconnect_error)
                    raise

    finally:
        consumer.close()
        event_repo.close()
        session_repo.close()
        logger.info("PostgreSQL consumer shutdown complete.")


def _run_opensearch_consumer(settings) -> None:
    """Run the OpenSearch consumer pipeline."""
    global _shutdown_requested

    from confluent_kafka import Consumer, KafkaError
    from clickstream.infrastructure.search.opensearch import OpenSearchRepository
    from clickstream.utils.session_state import set_last_message_timestamp

    group_id = settings.opensearch.consumer_group_id
    topic = settings.kafka.events_topic
    batch_size = settings.consumer.batch_size
    poll_timeout = settings.consumer.poll_timeout_ms / 1000.0

    # Build consumer config
    config = _build_consumer_config(group_id)
    consumer = Consumer(config)
    consumer.subscribe([topic])

    # Initialize OpenSearch repository
    opensearch_repo = OpenSearchRepository(settings)
    opensearch_repo.connect()

    logger.info("Starting OpenSearch consumer (Mage AI)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", topic)
    logger.info("Index: %s", settings.opensearch.events_index)

    try:
        while not _shutdown_requested:
            # Consume messages
            messages = consumer.consume(
                num_messages=batch_size,
                timeout=poll_timeout,
            )

            if not messages:
                continue

            # Parse messages
            events = []
            for msg in messages:
                if msg.error():
                    error = msg.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error("Consumer error: %s", error)
                        continue

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    events.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Failed to decode message: %s", e)
                    continue

            if not events:
                continue

            try:
                # Index to OpenSearch
                opensearch_repo.save(events)

                # Track activity for status display
                set_last_message_timestamp(group_id)

                # Commit offsets
                consumer.commit()

                logger.debug("Indexed batch: %d events", len(events))

            except Exception as e:
                logger.error("Error processing batch: %s", e)

                try:
                    opensearch_repo.close()
                    opensearch_repo.connect()
                    logger.info("Reconnected to OpenSearch after error")
                except Exception as reconnect_error:
                    logger.error("Failed to reconnect: %s", reconnect_error)
                    raise

    finally:
        consumer.close()
        opensearch_repo.close()
        logger.info("OpenSearch consumer shutdown complete.")
