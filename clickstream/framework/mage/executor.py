# ==============================================================================
# Mage Pipeline Executor
# ==============================================================================
"""
Mage pipeline executor.

Provides execute_streaming_pipeline() to run Mage streaming pipelines
continuously until interrupted.

Uses the correct Mage API: mage_ai.data_preparation.models.pipeline.Pipeline
with execute_sync() for streaming pipelines.
"""

import contextlib
import io
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def get_mage_project_path() -> str:
    """Get the path to the Mage project directory."""
    from clickstream.utils.paths import get_project_root

    return str(get_project_root() / "clickstream" / "framework" / "mage")


def execute_streaming_pipeline(
    pipeline_name: str,
    variables: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Execute a Mage streaming pipeline (continuous execution).

    For streaming pipelines, this runs indefinitely until interrupted.
    Uses Kafka consumer groups for automatic partition assignment.

    Args:
        pipeline_name: Name of the streaming pipeline (e.g., "postgresql_consumer")
        variables: Optional pipeline variables

    Raises:
        ImportError: If mage-ai is not installed
        RuntimeError: If pipeline not found or execution fails
    """
    project_path = get_mage_project_path()

    # Change to Mage project directory - REQUIRED for Mage to find pipelines
    os.chdir(project_path)

    # Import Mage, suppressing "DBT library not installed" message
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            from mage_ai.data_preparation.models.pipeline import Pipeline
    except ImportError as e:
        raise ImportError("Mage AI not installed. Install with: pip install -e '.[mage]'") from e

    # Load pipeline
    pipeline = Pipeline.get(pipeline_name)
    if pipeline is None:
        raise RuntimeError(f"Pipeline '{pipeline_name}' not found in {project_path}")

    logger.info("Pipeline: %s", pipeline_name)

    # Execute continuously (streaming mode)
    # execute_sync() runs indefinitely for streaming pipelines
    pipeline.execute_sync(global_vars=variables or {})
