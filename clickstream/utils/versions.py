# ==============================================================================
# Version Utilities
# ==============================================================================
"""
Utilities for retrieving package versions.
"""

from importlib.metadata import version, PackageNotFoundError


def get_clickstream_version() -> str:
    """
    Get the clickstream-demo package version.

    Returns:
        Version string (e.g., "0.1.0")
    """
    try:
        return version("clickstream-demo")
    except PackageNotFoundError:
        return "0.1.0"


def get_package_version(package_name: str) -> str:
    """
    Get the version of an installed package.

    Args:
        package_name: Name of the package (e.g., "quixstreams", "mage-ai")

    Returns:
        Version string or "unknown" if not found
    """
    try:
        return version(package_name)
    except PackageNotFoundError:
        return "unknown"
