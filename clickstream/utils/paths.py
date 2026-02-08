# ==============================================================================
# Path Constants and Utilities
# ==============================================================================
"""
Centralized path constants for PID files, log files, and project paths.

This module provides:
- Producer PID and log file paths
- Consumer PID and log file path generators
- Project root detection
- Schema and script paths
"""

from pathlib import Path

# ==============================================================================
# Temporary File Paths
# ==============================================================================

# Producer files
PRODUCER_PID_FILE = Path("/tmp/producer.pid")
PRODUCER_LOG_FILE = Path("/tmp/producer.log")


def get_consumer_pid_file(instance: int, consumer_type: str) -> Path:
    """
    Get PID file path for a consumer instance.

    Args:
        instance: Consumer instance number
        consumer_type: Type of consumer (e.g., "postgresql", "opensearch")

    Returns:
        Path to the PID file
    """
    return Path(f"/tmp/consumer_{instance}_{consumer_type}.pid")


def get_consumer_log_file(instance: int, consumer_type: str) -> Path:
    """
    Get log file path for a consumer instance.

    Args:
        instance: Consumer instance number
        consumer_type: Type of consumer (e.g., "postgresql", "opensearch")

    Returns:
        Path to the log file
    """
    return Path(f"/tmp/consumer_{instance}_{consumer_type}.log")


# ==============================================================================
# Project Structure Paths
# ==============================================================================


def get_project_root() -> Path:
    """
    Get the project root directory.

    Searches upward from the current file for a directory containing
    pyproject.toml. Falls back to current working directory if not found.

    Returns:
        Path to the project root directory
    """
    # Start from this file's location and walk up
    current = Path(__file__).parent.parent.parent  # utils/paths.py -> clickstream -> project
    if (current / "pyproject.toml").exists():
        return current

    # Fall back to cwd if it looks like a project root
    cwd = Path.cwd()
    if (cwd / "pyproject.toml").exists():
        return cwd

    return cwd


def get_schema_dir() -> Path:
    """
    Get the schema directory containing SQL scripts.

    Returns:
        Path to the schema directory
    """
    return get_project_root() / "schema"


def get_init_sql_path() -> Path:
    """
    Get the path to the database initialization SQL script.

    Returns:
        Path to init.sql
    """
    return get_schema_dir() / "init.sql"


def get_certs_dir() -> Path:
    """
    Get the certificates directory for SSL/TLS connections.

    Returns:
        Path to the certs directory
    """
    return get_project_root() / "certs"


def get_mage_project_path() -> Path:
    """
    Get the Mage AI project directory.

    Returns:
        Path to the Mage project within the framework directory
    """
    return get_project_root() / "clickstream" / "framework" / "mage"
