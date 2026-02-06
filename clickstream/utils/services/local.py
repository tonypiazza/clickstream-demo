# ==============================================================================
# Local Service Health Check
# ==============================================================================
"""
Service readiness for local Docker/dev environments.

Uses direct connections to check if services are running. Services are
either "running" or "unreachable" — there are no intermediate states
like "rebuilding" in local environments.
"""

import logging

from clickstream.base.service_health import ServiceHealthCheck, ServiceStatus

logger = logging.getLogger(__name__)


class LocalHealthCheck(ServiceHealthCheck):
    """
    Service readiness for local Docker/dev environments.

    Uses direct connections — the same check_kafka_connection(),
    check_db_connection(), check_valkey_connection() functions already
    in the codebase. Services are either "running" or "unreachable".
    """

    def check_service(self, service_type: str) -> ServiceStatus:
        """
        Check service status via direct connection.

        Args:
            service_type: One of "kafka", "pg", "valkey", "opensearch"

        Returns:
            ServiceStatus with "running" or "unreachable"
        """
        if service_type == "kafka":
            from clickstream.cli.shared import check_kafka_connection

            ok = check_kafka_connection()
            return ServiceStatus("kafka", "running" if ok else "unreachable")

        elif service_type == "pg":
            from clickstream.cli.shared import check_db_connection

            ok = check_db_connection()
            return ServiceStatus("pg", "running" if ok else "unreachable")

        elif service_type == "valkey":
            from clickstream.utils.session_state import check_valkey_connection

            ok = check_valkey_connection()
            return ServiceStatus("valkey", "running" if ok else "unreachable")

        elif service_type == "opensearch":
            from clickstream.infrastructure.search.opensearch import (
                check_opensearch_connection,
            )

            ok = check_opensearch_connection()
            return ServiceStatus("opensearch", "running" if ok else "unreachable")

        else:
            logger.warning("Unknown service type: %s", service_type)
            return ServiceStatus(service_type, "unknown")
