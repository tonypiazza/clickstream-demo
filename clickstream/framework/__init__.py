# ==============================================================================
# Framework Abstraction Layer
# ==============================================================================
"""
Framework abstraction layer for streaming implementations.

Provides get_framework() to get the active streaming framework based on
the STREAMING_IMPL environment variable.

Supported frameworks:
- quix: Quix Streams (default) - recommended for production
- mage: Mage AI - alternative streaming framework
- bytewax: Bytewax - Python streaming with Rust performance
- default: Raw kafka-python - lightweight baseline implementation

Usage:
    from clickstream.framework import get_framework

    framework = get_framework()
    framework.run_consumer("postgresql", instance=0)
    framework.run_producer(limit=1000)
"""

from clickstream.framework.base import StreamingFramework


def get_framework() -> StreamingFramework:
    """
    Get the streaming framework instance based on STREAMING_IMPL env var.

    The framework is determined by the STREAMING_IMPL environment variable:
    - "quix" (default): Uses Quix Streams for Kafka consumption
    - "mage": Uses Mage AI pipelines for streaming
    - "bytewax": Uses Bytewax dataflows for streaming
    - "default": Uses raw kafka-python (baseline implementation)

    Returns:
        StreamingFramework: The active framework instance

    Raises:
        ImportError: If the selected framework package is not installed
        ValueError: If an unknown framework is specified

    Example:
        >>> from clickstream.framework import get_framework
        >>> framework = get_framework()
        >>> print(framework.name)
        'Quix Streams'
    """
    from clickstream.utils.config import get_settings

    impl = get_settings().streaming.impl

    match impl:
        case "quix":
            from clickstream.framework.quix import QuixFramework

            return QuixFramework()
        case "mage":
            try:
                from clickstream.framework.mage import MageFramework

                return MageFramework()
            except ImportError as e:
                raise ImportError(
                    "Mage framework selected but 'mage-ai' not installed.\n"
                    "Install with: pip install -e '.[mage]'"
                ) from e
        case "bytewax":
            try:
                from clickstream.framework.bytewax import BytewaxFramework

                return BytewaxFramework()
            except ImportError as e:
                raise ImportError(
                    "Bytewax framework selected but 'bytewax' not installed.\n"
                    "Install with: pip install -e '.[bytewax]'"
                ) from e
        case "default":
            from clickstream.framework.default import DefaultFramework

            return DefaultFramework()
        case _:
            raise ValueError(
                f"Unknown streaming framework: '{impl}'.\nValid options are: quix, mage, bytewax, default"
            )


__all__ = ["get_framework", "StreamingFramework"]
