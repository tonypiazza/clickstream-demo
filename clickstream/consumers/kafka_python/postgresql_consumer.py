# ==============================================================================
# kafka-python PostgreSQL Consumer
# ==============================================================================
"""
PostgreSQL consumer implementation using kafka-python.

This is the baseline consumer implementation with no external streaming
framework dependencies. Uses:
- kafka-python for Kafka consumption with manual offset commits
- PostgreSQLEventRepository for event persistence
- SessionState for Valkey-based session management
- PostgreSQLSessionRepository for session persistence

Other frameworks can delegate to this consumer when they don't have
their own implementation.
"""

import json
import logging
import signal

from clickstream.infrastructure.kafka import build_kafka_config
from clickstream.infrastructure.repositories.postgresql import (
    PostgreSQLEventRepository,
    PostgreSQLSessionRepository,
)
from clickstream.utils.config import get_settings
from clickstream.utils.session_state import (
    SessionState,
    get_valkey_client,
    set_last_message_timestamp,
)

logger = logging.getLogger(__name__)

# Global flag for shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals by setting flag."""
    global _shutdown_requested
    logger.info("Received signal %d, shutting down gracefully...", signum)
    _shutdown_requested = True


def run() -> None:
    """
    Run the PostgreSQL consumer using kafka-python.

    This function:
    1. Creates a KafkaConsumer with manual offset commits
    2. Initializes Valkey session state manager
    3. Processes events in batches:
       - Saves events to PostgreSQL
       - Updates sessions in Valkey (batch operation)
       - Saves sessions to PostgreSQL
       - Tracks activity timestamp
       - Commits offsets

    Performance note: Uses batch_update_sessions() for Valkey operations,
    which is critical for remote Valkey servers (e.g., Aiven) where network
    latency would otherwise limit throughput.
    """
    global _shutdown_requested
    _shutdown_requested = False

    # Set up signal handlers
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    from kafka import KafkaConsumer

    settings = get_settings()
    consumer_settings = settings.consumer
    group_id = settings.postgresql_consumer.group_id

    # Build Kafka consumer config
    kafka_config = build_kafka_config(settings.kafka)
    consumer = KafkaConsumer(
        settings.kafka.events_topic,
        **kafka_config,
        group_id=group_id,
        auto_offset_reset=consumer_settings.auto_offset_reset,
        enable_auto_commit=False,  # Manual commits after batch
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

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

    logger.info("Starting PostgreSQL consumer (kafka-python)...")
    logger.info("Consumer group: %s", group_id)
    logger.info("Topic: %s", settings.kafka.events_topic)

    try:
        while not _shutdown_requested:
            # Poll batch of messages
            raw_messages = consumer.poll(
                timeout_ms=consumer_settings.poll_timeout_ms,
                max_records=consumer_settings.batch_size,
            )

            if not raw_messages:
                continue

            # Flatten partition messages into event list
            events = []
            for partition_messages in raw_messages.values():
                for msg in partition_messages:
                    events.append(msg.value)

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
