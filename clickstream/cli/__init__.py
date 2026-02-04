# ==============================================================================
# CLI Commands Module
# ==============================================================================
"""
CLI commands for the clickstream pipeline.

Commands are organized into separate modules for maintainability:
- shared.py: Common utilities, constants, and helpers
- status.py: Status command showing pipeline health
"""

from clickstream.cli.shared import (
    # Constants
    BOX_WIDTH,
    PRODUCER_LOG_FILE,
    PRODUCER_PID_FILE,
    # Classes
    Box,
    Colors,
    Icons,
    # Aliases
    B,
    C,
    I,
    # Box drawing helpers (private)
    _box_bottom,
    _box_header,
    _box_line,
    _empty_line,
    _section_header,
    _section_header_plain,
    _status_badge,
    _visible_len,
    # Path helpers
    get_consumer_log_file,
    get_consumer_pid_file,
    get_opensearch_instance,
    get_mage_project_path,
    get_project_root,
    # Framework helpers
    get_active_framework_name,
    # Process management
    get_process_end_time,
    get_process_pid,
    get_process_start_time,
    is_process_running,
    start_background_process,
    stop_process,
    # Multi-consumer management
    count_running_consumers,
    get_all_consumer_pids,
    get_topic_partition_count,
    start_consumer_instance,
    stop_all_consumers,
    # OpenSearch consumer management
    is_opensearch_consumer_running,
    start_opensearch_consumer,
    stop_opensearch_consumer,
    # Kafka helpers
    get_kafka_admin_client,
    get_kafka_config,
    purge_kafka_topic,
    reset_consumer_group,
    # Database helpers
    check_db_connection,
    check_kafka_connection,
)

__all__ = [
    # Constants
    "BOX_WIDTH",
    "PRODUCER_LOG_FILE",
    "PRODUCER_PID_FILE",
    # Classes
    "Box",
    "Colors",
    "Icons",
    # Aliases
    "B",
    "C",
    "I",
    # Box drawing helpers (private - kept for internal use)
    "_box_bottom",
    "_box_header",
    "_box_line",
    "_empty_line",
    "_section_header",
    "_section_header_plain",
    "_status_badge",
    "_visible_len",
    # Path helpers
    "get_consumer_log_file",
    "get_consumer_pid_file",
    "get_opensearch_instance",
    "get_mage_project_path",
    "get_project_root",
    # Framework helpers
    "get_active_framework_name",
    # Process management
    "get_process_end_time",
    "get_process_pid",
    "get_process_start_time",
    "is_process_running",
    "start_background_process",
    "stop_process",
    # Multi-consumer management
    "count_running_consumers",
    "get_all_consumer_pids",
    "get_topic_partition_count",
    "start_consumer_instance",
    "stop_all_consumers",
    # OpenSearch consumer management
    "is_opensearch_consumer_running",
    "start_opensearch_consumer",
    "stop_opensearch_consumer",
    # Kafka helpers
    "get_kafka_admin_client",
    "get_kafka_config",
    "purge_kafka_topic",
    "reset_consumer_group",
    # Database helpers
    "check_db_connection",
    "check_kafka_connection",
]
