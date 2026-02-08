# ==============================================================================
# Service Health Check Abstract Base Class
# ==============================================================================
"""
Abstract interface for checking service readiness.

Each environment (local, Aiven, etc.) implements this differently.
Local environments use direct connections; Aiven uses the REST API
for fast status checks without 30s mTLS timeouts.

Implementations: LocalHealthCheck, AivenHealthCheck
"""

import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class ServiceStatus:
    """Status of a single service.

    Attributes:
        name: Service identifier (e.g., "kafka", "pg", "valkey", "opensearch")
        state: Current state (e.g., "running", "rebuilding", "poweroff", "unreachable")
    """

    name: str
    state: str

    @property
    def is_running(self) -> bool:
        """Whether the service is in a running state."""
        return self.state == "running"


class ServiceHealthCheck(ABC):
    """
    Abstract interface for checking service readiness.

    Each environment (local, Aiven, etc.) implements this differently.
    Supported service types: "kafka", "pg", "valkey", "opensearch".
    """

    @abstractmethod
    def check_service(self, service_type: str) -> ServiceStatus:
        """
        Check the status of a single service.

        Args:
            service_type: One of "kafka", "pg", "valkey", "opensearch"

        Returns:
            ServiceStatus with current state
        """
        ...

    def check_required_services(self, service_types: list[str]) -> dict[str, ServiceStatus]:
        """
        Check multiple services.

        Default implementation calls check_service() for each.

        Args:
            service_types: List of service type identifiers

        Returns:
            Dict mapping service type to its status
        """
        return {svc: self.check_service(svc) for svc in service_types}

    def wait_until_ready(
        self,
        service_types: list[str],
        timeout: float = 300.0,
        poll_interval: float = 10.0,
        on_status: Callable[[dict[str, ServiceStatus]], None] | None = None,
    ) -> dict[str, ServiceStatus]:
        """
        Poll until all services are running or timeout.

        Default implementation polls check_required_services() in a loop.
        Subclasses can override for environment-specific behavior.

        Args:
            service_types: Services to wait for (e.g., ["kafka", "pg", "valkey"])
            timeout: Maximum wait time in seconds (default: 300 = 5 minutes)
            poll_interval: Seconds between polls (default: 10)
            on_status: Optional callback for UI updates, called with current statuses

        Returns:
            Final service statuses (all running)

        Raises:
            TimeoutError: If services don't become ready within timeout
        """
        start = time.monotonic()
        while True:
            statuses = self.check_required_services(service_types)
            if on_status:
                on_status(statuses)
            if all(s.is_running for s in statuses.values()):
                return statuses
            elapsed = time.monotonic() - start
            if elapsed >= timeout:
                not_ready = {k: v for k, v in statuses.items() if not v.is_running}
                raise TimeoutError(
                    f"Services not ready after {timeout:.0f}s: "
                    + ", ".join(f"{k}={v.state}" for k, v in not_ready.items())
                )
            time.sleep(poll_interval)
