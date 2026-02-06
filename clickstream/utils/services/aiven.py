# ==============================================================================
# Aiven Service Health Check
# ==============================================================================
"""
Service readiness for Aiven cloud environments.

Uses the Aiven REST API for fast status checks (~1-2s vs 30s for direct
Kafka mTLS timeouts). Services can be in states like RUNNING, REBUILDING,
POWEROFF, REBALANCING, etc.

wait_until_ready() inherits the default polling behavior from the ABC,
which naturally handles the REBUILDING -> RUNNING transition by polling
check_service() every poll_interval seconds.
"""

import logging

from clickstream.base.service_health import ServiceHealthCheck, ServiceStatus

logger = logging.getLogger(__name__)


class AivenHealthCheck(ServiceHealthCheck):
    """
    Service readiness for Aiven cloud environments.

    Uses the Aiven REST API for fast status checks. The API returns the
    service state (RUNNING, REBUILDING, POWEROFF, etc.) without needing
    a direct connection, avoiding slow mTLS handshake timeouts.
    """

    def __init__(self, settings) -> None:
        self._settings = settings

    def check_service(self, service_type: str) -> ServiceStatus:
        """
        Check service status via Aiven REST API.

        Args:
            service_type: One of "kafka", "pg", "valkey", "opensearch"

        Returns:
            ServiceStatus with current Aiven state (e.g., "running", "rebuilding")
        """
        from clickstream.utils.aiven import get_service_status

        service_name = self._settings.aiven.get_service_name(service_type)
        if not service_name:
            logger.warning("No Aiven service name configured for %s", service_type)
            return ServiceStatus(service_type, "unknown")

        status = get_service_status(service_name)
        if status is None:
            logger.warning("Aiven API unreachable for %s (%s)", service_type, service_name)
            return ServiceStatus(service_type, "unknown")

        state = status.get("state", "unknown")
        return ServiceStatus(service_type, state.lower())
