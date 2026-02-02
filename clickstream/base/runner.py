# ==============================================================================
# Base Runner Abstract Class
# ==============================================================================
"""
Base runner with common lifecycle management.

Provides signal handling, logging setup, and shutdown coordination.
Framework-specific runners extend this and implement _run().
"""

import logging
import signal
from abc import ABC, abstractmethod
from typing import final

logger = logging.getLogger(__name__)


class BaseRunner(ABC):
    """Base runner with common lifecycle management."""

    def __init__(self):
        self._shutdown_requested = False

    @final
    def run(self) -> None:
        """Main entry point with signal handling."""
        self._setup_signal_handlers()
        self._setup_logging()

        try:
            self._run()
        except KeyboardInterrupt:
            logger.info("Runner interrupted by keyboard")
        finally:
            self._cleanup()

    @abstractmethod
    def _run(self) -> None:
        """Framework-specific run implementation."""
        ...

    def _setup_signal_handlers(self) -> None:
        """Common signal handling - can be overridden."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame) -> None:
        """Handle shutdown signals."""
        logger.info("Received signal %d, requesting shutdown...", signum)
        self._shutdown_requested = True
        self._on_shutdown_requested()

    def _setup_logging(self) -> None:
        """Common logging setup - can be overridden."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    def _on_shutdown_requested(self) -> None:
        """Hook for frameworks to handle shutdown. Optional override."""
        pass

    def _cleanup(self) -> None:
        """Cleanup resources. Optional override."""
        pass

    @property
    def shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_requested
