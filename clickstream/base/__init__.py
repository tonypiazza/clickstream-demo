# ==============================================================================
# Base Abstract Classes
# ==============================================================================
"""
Abstract base classes defining loose contracts for the ports-and-adapters architecture.

These ABCs serve as guidance - frameworks can adapt them to fit their paradigms.

Note: Process management (starting/stopping consumers/producers) is intentionally
not abstracted here. Process orchestration is inherently framework-specific and
doesn't map to a common interface across Quix, Mage, Bytewax, etc.
"""

from clickstream.base.cache import Cache
from clickstream.base.consumer import BaseConsumer
from clickstream.base.producer import BaseProducer
from clickstream.base.repositories import (
    EventRepository,
    SearchRepository,
    SessionRepository,
)
from clickstream.base.runner import BaseRunner
from clickstream.base.service_health import ServiceHealthCheck, ServiceStatus
from clickstream.base.session_state import SessionStateStore
from clickstream.base.sinks import BaseSink

__all__ = [
    "BaseConsumer",
    "BaseProducer",
    "BaseRunner",
    "BaseSink",
    "Cache",
    "EventRepository",
    "SearchRepository",
    "ServiceHealthCheck",
    "ServiceStatus",
    "SessionRepository",
    "SessionStateStore",
]
