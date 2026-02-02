# ==============================================================================
# Core Domain Logic
# ==============================================================================
"""
Pure domain logic with no external dependencies.

This module contains:
- Domain models (ClickstreamEvent, Session, EventType)
- Session processing logic (aggregation, timeout detection)

All code here is framework-agnostic and easily unit-testable.
"""

from clickstream.core.models import ClickstreamEvent, EventType, Session
from clickstream.core.session_processor import SessionProcessor

__all__ = [
    "ClickstreamEvent",
    "EventType",
    "Session",
    "SessionProcessor",
]
