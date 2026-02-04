# ==============================================================================
# Confluent Kafka PostgreSQL Consumer
# ==============================================================================
"""
PostgreSQL consumer implementation using confluent-kafka (librdkafka C wrapper).

This is the highest-performance consumer option, using the native C library
for Kafka protocol handling. Key optimizations over kafka-python:

- Uses consume() batch API for efficient message retrieval
- Optimized fetch settings for higher throughput
- Pre-fetching with queued.min.messages for continuous processing
- Native C library handles protocol efficiently

Uses:
- confluent-kafka for Kafka consumption with manual offset commits
- PostgreSQLEventRepository for event persistence
- SessionState for Valkey-based session management
- PostgreSQLSessionRepository for session persistence
"""

import json
import logging
import signal

from clickstream.utils.config import get_settings
from clickstream.utils.paths import get_project_root

logger = logging.getLogger(__name__)

# Global flag for shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals by setting flag."""
    global _shutdown_requested
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown_requested = True


def _build_consumer_config(kafka_settings, group_id: str, auto_offset_reset: str) -> dict:
    """
    Build confluent-kafka consumer configuration.

    Includes performance optimizations for high-throughput consumption.

    Args:
        kafka_settings: KafkaSettings instance
        group_id: Consumer group ID
        auto_offset_reset: Offset reset policy ('earliest', 'latest')

    Returns:
        Dict with confluent-kafka consumer configuration
    """
    config = {
        "bootstrap.servers": kafka_settings.bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,  # Manual commit for reliability
        # Performance tuning - fetch settings
        "fetch.min.bytes": 1024,  # Wait for at least 1KB before returning
        "fetch.max.bytes": 52428800,  # 50MB max fetch size
        "max.partition.fetch.bytes": 1048576,  # 1MB per partition
        # Pre-fetching for continuous processing
        "queued.min.messages": 10000,  # Pre-fetch queue size
        "queued.max.messages.kbytes": 65536,  # 64MB pre-fetch buffer
        # Session management
        "session.timeout.ms": 45000,
        "heartbeat.interval.ms": 15000,
        # Reduce rebalance overhead
        "max.poll.interval.ms": 300000,  # 5 minutes max processing time
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


def run() -> None:
    """
    Run the PostgreSQL consumer using confluent-kafka.

    This function:
    1. Creates a confluent-kafka Consumer with manual offset commits
    2. Initializes Valkey session state manager
    3. Processes events in batches:
       - Saves events to PostgreSQL
       - Updates sessions in Valkey (batch operation)
       - Saves sessions to PostgreSQL
       - Tracks activity timestamp
       - Commits offsets

    Performance optimizations:
    - Uses consume() batch API instead of poll()
    - Optimized fetch settings for higher throughput
    - Pre-fetching for continuous processing
    """
    global _shutdown_requested
    _shutdown_requested = False

    # Set up signal handlers
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

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

    settings = get_settings()
    consumer_settings = settings.consumer
    group_id = settings.postgresql_consumer.group_id
    topic = settings.kafka.events_topic

    # Build confluent-kafka consumer config with optimizations
    kafka_config = _build_consumer_config(
        settings.kafka,
        group_id,
        consumer_settings.auto_offset_reset,
    )
    consumer = Consumer(kafka_config)
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

    logger.info("Starting PostgreSQL consumer (confluent-kafka)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", topic)

    try:
        while not _shutdown_requested:
            # Use consume() batch API - more efficient than poll()
            # Returns list of messages directly (not dict of partition -> messages)
            messages = consumer.consume(
                num_messages=consumer_settings.batch_size,
                timeout=consumer_settings.poll_timeout_ms / 1000.0,  # Convert to seconds
            )

            if not messages:
                continue

            # Process messages, filtering out errors
            events = []
            for msg in messages:
                if msg.error():
                    error = msg.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    else:
                        logger.error("Consumer error: %s", error)
                        continue

                # Deserialize JSON message
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

                # 2. Update sessions in Valkey (batch operation - 2 round-trips)
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
                # Rollback any partial transactions
                event_repo.rollback()
                session_repo.rollback()

                # Attempt to reconnect on connection errors
                try:
                    event_repo.reconnect()
                    session_repo.reconnect()
                    logger.info("Reconnected to PostgreSQL after error")
                except Exception as reconnect_error:
                    logger.error("Failed to reconnect: %s", reconnect_error)
                    raise

    except Exception as e:
        logger.exception("Consumer error: %s", e)
        raise

    finally:
        consumer.close()
        event_repo.close()
        session_repo.close()
        logger.info("PostgreSQL consumer shutdown complete.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
