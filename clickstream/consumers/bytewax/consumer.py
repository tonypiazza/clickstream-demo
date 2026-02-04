# ==============================================================================
# Bytewax Consumer
# ==============================================================================
"""
Bytewax consumer implementation.

Uses Bytewax's dataflow API with custom sinks for efficient batch processing.
"""

from clickstream.consumers.base import ConsumerType, StreamingConsumer
from clickstream.utils.versions import get_package_version


class BytewaxConsumer(StreamingConsumer):
    """
    Bytewax consumer implementation.

    Uses Bytewax's dataflow API with custom sinks for efficient batch processing.
    Bytewax manages parallelism internally via workers_per_process, so only
    one OS process is started. The single process spawns multiple workers,
    one per Kafka partition.
    """

    @property
    def name(self) -> str:
        return "bytewax"

    @property
    def version(self) -> str:
        return f"bytewax v{get_package_version('bytewax')}"

    @property
    def num_instances(self) -> int:
        """Bytewax runs as a single process with internal workers."""
        return 1

    @property
    def num_workers(self) -> int:
        """Bytewax spawns one worker per partition inside the single process."""
        return self._num_partitions

    def run(self) -> None:
        """Run the Bytewax consumer."""
        if self._consumer_type == "postgresql":
            from clickstream.consumers.bytewax.postgresql_consumer import run

            run()
        elif self._consumer_type == "opensearch":
            from clickstream.consumers.bytewax.opensearch_consumer import run

            run()
        else:
            raise ValueError(f"Unknown consumer type: {self._consumer_type}")
