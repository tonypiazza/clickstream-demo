# ==============================================================================
# Database Repository Adapters
# ==============================================================================
"""
Database adapters implementing the repository interfaces from base/repositories.py.

Currently supported:
- PostgreSQL (postgresql.py)

Future:
- ClickHouse, DuckDB, BigQuery, etc.
"""

from clickstream.infrastructure.repositories.postgresql import (
    PostgreSQLEventRepository,
    PostgreSQLSessionRepository,
    check_postgresql_connection,
)

__all__ = [
    "PostgreSQLEventRepository",
    "PostgreSQLSessionRepository",
    "check_postgresql_connection",
]
