# ==============================================================================
# Consumer Commands
# ==============================================================================
"""
Consumer commands for the clickstream pipeline CLI.

Commands for starting, stopping, restarting, and viewing logs for the
Kafka consumers (PostgreSQL and OpenSearch).
"""

import time
from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import typer


class ConsumerType(str, Enum):
    """Consumer type for --type option."""

    all = "all"
    postgresql = "postgresql"
    opensearch = "opensearch"


from clickstream.cli.shared import (
    C,
    I,
    count_running_consumers,
    get_all_consumer_pids,
    get_consumer_log_file,
    get_consumer_pid_file,
    get_opensearch_instance,
    get_topic_partition_count,
    is_opensearch_consumer_running,
    start_consumer_instance,
    start_opensearch_consumer,
    stop_all_consumers,
    stop_opensearch_consumer,
    get_process_pid,
    get_project_root,
)
from clickstream.utils.config import get_settings


# ==============================================================================
# Helper Functions
# ==============================================================================


def _show_log(log_file: Path, label: str, follow: bool, lines: int) -> None:
    """Display a log file's contents."""
    import subprocess

    if follow:
        print(f"{C.DIM}Following {log_file} (Ctrl+C to stop)...{C.RESET}")
        print()
        try:
            subprocess.run(["tail", "-f", str(log_file)], check=False)
        except KeyboardInterrupt:
            print()
    else:
        # Read last N lines
        with open(log_file) as f:
            all_lines = f.readlines()

        if not all_lines:
            print(f"{C.DIM}Log file is empty{C.RESET}")
            return

        display_lines = all_lines[-lines:]
        print(f"{C.DIM}=== {label} (last {len(display_lines)} lines) ==={C.RESET}")
        print()
        for line in display_lines:
            print(line, end="")


def _check_package_installed(package_name: str) -> bool:
    """Check if a Python package is installed."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        version(package_name)
        return True
    except PackageNotFoundError:
        return False


def _get_package_version_safe(package_name: str) -> str | None:
    """Get package version or None if not installed."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(package_name)
    except PackageNotFoundError:
        return None


# ==============================================================================
# Commands
# ==============================================================================


def consumer_list() -> None:
    """List all available consumer implementations.

    Shows all consumer options with version info and indicates which one
    is currently active.

    Examples:
        clickstream consumer list
    """
    from clickstream.utils.config import get_settings

    settings = get_settings()
    current_impl = settings.consumer.impl

    # Define available consumers: (impl_key, package_name, description)
    consumers = [
        ("confluent", "confluent-kafka", "librdkafka C library (fastest)"),
        ("kafka_python", "kafka-python", "Pure Python Kafka consumer"),
        ("quix", "quixstreams", "Quix Streams consumer API"),
        ("mage", "mage-ai", "Mage AI streaming pipelines"),
        ("bytewax", "bytewax", "Bytewax dataflow engine"),
    ]

    print()
    print(f"  {C.BOLD}Available Consumers:{C.RESET}")
    print()

    for impl, package_name, description in consumers:
        # Check if this is the active implementation
        if impl == current_impl:
            marker = f"{C.BRIGHT_GREEN}{I.CHECK}{C.RESET}"
        else:
            marker = " "

        # Get version or installation status
        pkg_version = _get_package_version_safe(package_name)
        if pkg_version:
            version_str = f"v{pkg_version}"
            status_color = C.WHITE
        else:
            version_str = "(not installed)"
            status_color = C.DIM

        # Format: marker impl_name version - description
        print(
            f"  {marker} {status_color}{impl:14}{C.RESET} "
            f"{C.DIM}{version_str:16}{C.RESET} {C.DIM}{description}{C.RESET}"
        )

    print()
    print(f"  {C.DIM}Currently active:{C.RESET} {C.WHITE}{current_impl}{C.RESET}")
    print(f"  {C.DIM}Set via:{C.RESET} CONSUMER_IMPL environment variable")
    print()


def consumer_start(
    truncate_log: Annotated[
        bool, typer.Option("--truncate-log", "-t", help="Truncate log files before starting")
    ] = False,
    consumer_type: Annotated[
        ConsumerType,
        typer.Option("--type", "-T", help="Which consumer(s) to start"),
    ] = ConsumerType.all,
) -> None:
    """Start the consumer pipeline as background processes.

    Spawns consumer processes for parallel processing. The number of processes
    is determined by the consumer implementation:
    - Traditional consumers (confluent, kafka_python): one process per partition
    - Dataflow consumers (bytewax): single process with internal workers

    Use --type to start only specific consumers:
    - all (default): Start both PostgreSQL and OpenSearch (if enabled)
    - postgresql: Start only PostgreSQL consumers
    - opensearch: Start only OpenSearch consumer (requires OPENSEARCH_ENABLED=true)

    Use 'clickstream consumer stop' to stop all consumer instances.

    Examples:
        clickstream consumer start
        clickstream consumer start --type postgresql    # Start only PostgreSQL consumers
        clickstream consumer start --type opensearch    # Start only OpenSearch consumer
        clickstream consumer start --truncate-log       # Clear logs before starting
    """
    import glob as glob_module

    settings = get_settings()

    start_postgresql = consumer_type in (ConsumerType.all, ConsumerType.postgresql)
    start_opensearch = consumer_type in (ConsumerType.all, ConsumerType.opensearch)

    # Validate OpenSearch consumer type requires OPENSEARCH_ENABLED=true
    if consumer_type == ConsumerType.opensearch and not settings.opensearch.enabled:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} OpenSearch is not enabled. "
            f"Set OPENSEARCH_ENABLED=true to start OpenSearch consumer{C.RESET}"
        )
        raise typer.Exit(1)

    # Get consumer to determine parallelism model
    from clickstream.consumers import get_consumer

    pg_consumer = get_consumer("postgresql")
    num_instances = pg_consumer.num_instances

    # Check if already running (only for requested type)
    running = count_running_consumers()
    opensearch_running = settings.opensearch.enabled and is_opensearch_consumer_running()

    if start_postgresql and running > 0:
        pids = get_all_consumer_pids()
        print(
            f"{C.BRIGHT_YELLOW}{I.STOP} PostgreSQL consumer is already running ({running} instances){C.RESET}"
        )
        for instance, pid in pids:
            print(f"  Instance {instance}: PID {C.WHITE}{pid}{C.RESET}")
        print(f"  Use '{C.DIM}clickstream consumer stop{C.RESET}' to stop")
        raise typer.Exit(1)

    if start_opensearch and opensearch_running:
        instance = get_opensearch_instance()
        os_pid_file = get_consumer_pid_file(instance, "opensearch")
        os_pid = get_process_pid(os_pid_file)
        print(
            f"{C.BRIGHT_YELLOW}{I.STOP} OpenSearch consumer is already running (PID {os_pid}){C.RESET}"
        )
        print(f"  Use '{C.DIM}clickstream consumer stop{C.RESET}' to stop")
        raise typer.Exit(1)

    # Check topic partition count if topic exists (only if starting PostgreSQL)
    if start_postgresql:
        topic = settings.kafka.events_topic
        num_partitions = pg_consumer.num_partitions
        existing_partitions = get_topic_partition_count(topic)
        if existing_partitions is not None and existing_partitions < num_partitions:
            print(
                f"{C.BRIGHT_RED}{I.CROSS} Topic '{topic}' has {existing_partitions} partitions, "
                f"but {num_partitions} are configured{C.RESET}"
            )
            print(
                f"  Run '{C.WHITE}clickstream data reset -y{C.RESET}' to recreate topic with correct partitions"
            )
            raise typer.Exit(1)

    # Truncate log files if requested
    if truncate_log:
        for log_path in glob_module.glob("/tmp/consumer_*_*.log"):
            Path(log_path).unlink()
        print(f"  Log files truncated")

    # Get paths
    project_root = get_project_root()

    # Initialize and start PostgreSQL consumers
    if start_postgresql:
        # Initialize PostgreSQL schema (once, before spawning consumers)
        print("  Making sure PostgreSQL schema has been initialized...", end=" ", flush=True)
        try:
            from clickstream.utils.db import ensure_schema

            ensure_schema()
            print("done")
        except Exception as e:
            print("failed")
            print(f"{C.BRIGHT_RED}{I.CROSS} Failed to initialize schema: {e}{C.RESET}")
            raise typer.Exit(1)

        print(f"  Starting PostgreSQL consumer ({pg_consumer.parallelism_description})...")

        runner_script = project_root / "clickstream" / "consumer_runner.py"

        # Start all instances (staggered to avoid Mage log directory race condition)
        for i in range(num_instances):
            start_consumer_instance(runner_script, i, project_root)
            if i < num_instances - 1:
                time.sleep(1)  # Stagger startup to avoid race condition

        # Wait for processes to start
        time.sleep(2)

        # Verify they started
        running = get_all_consumer_pids()
        if len(running) == num_instances:
            print(
                f"{C.BRIGHT_GREEN}{I.CHECK} PostgreSQL consumer started "
                f"({pg_consumer.parallelism_description}){C.RESET}"
            )
            print(f"  Logs:")
            for i in range(num_instances):
                log_file = get_consumer_log_file(i, "postgresql")
                print(f"    {C.DIM}{log_file}{C.RESET}")
        else:
            print(
                f"{C.BRIGHT_RED}{I.CROSS} Only {len(running)}/{num_instances} PostgreSQL consumers started{C.RESET}"
            )
            print(f"  Check logs for errors")
            raise typer.Exit(1)

    # Initialize and start OpenSearch consumer if enabled
    if start_opensearch and settings.opensearch.enabled:
        # Initialize OpenSearch index
        print("  Initializing OpenSearch index...", end=" ", flush=True)
        try:
            from opensearchpy import OpenSearch

            os_client = OpenSearch(
                hosts=[{"host": settings.opensearch.host, "port": settings.opensearch.port}],
                http_auth=(settings.opensearch.user, settings.opensearch.password),
                use_ssl=settings.opensearch.use_ssl,
                verify_certs=settings.opensearch.verify_certs,
                ssl_show_warn=False,
            )
            index_name = settings.opensearch.events_index
            if not os_client.indices.exists(index=index_name):
                index_body = {
                    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                    "mappings": {
                        "properties": {
                            "event_time": {"type": "date"},
                            "visitor_id": {"type": "keyword"},
                            "event": {"type": "keyword"},
                            "item_id": {"type": "keyword"},
                            "transaction_id": {"type": "keyword"},
                        }
                    },
                }
                os_client.indices.create(index=index_name, body=index_body)
            print("done")
        except Exception as e:
            print("failed")
            print(f"{C.BRIGHT_RED}{I.CROSS} Failed to initialize OpenSearch: {e}{C.RESET}")
            raise typer.Exit(1)

        print(f"  Starting OpenSearch consumer...")
        start_opensearch_consumer(project_root)
        time.sleep(2)

        if is_opensearch_consumer_running():
            instance = get_opensearch_instance()
            os_pid_file = get_consumer_pid_file(instance, "opensearch")
            os_log_file = get_consumer_log_file(instance, "opensearch")
            os_pid = get_process_pid(os_pid_file)
            print(f"{C.BRIGHT_GREEN}{I.CHECK} OpenSearch consumer started (PID {os_pid}){C.RESET}")
            print(f"  Log: {C.DIM}{os_log_file}{C.RESET}")
        else:
            instance = get_opensearch_instance()
            os_log_file = get_consumer_log_file(instance, "opensearch")
            print(f"{C.BRIGHT_RED}{I.CROSS} OpenSearch consumer failed to start{C.RESET}")
            print(f"  Check log: {C.DIM}{os_log_file}{C.RESET}")
            raise typer.Exit(1)


def consumer_stop(
    consumer_type: Annotated[
        ConsumerType,
        typer.Option("--type", "-T", help="Which consumer(s) to stop"),
    ] = ConsumerType.all,
) -> None:
    """Stop running consumer instances.

    Use --type to stop only specific consumers:
    - all (default): Stop both PostgreSQL and OpenSearch consumers
    - postgresql: Stop only PostgreSQL consumers
    - opensearch: Stop only OpenSearch consumer

    Examples:
        clickstream consumer stop
        clickstream consumer stop --type postgresql    # Stop only PostgreSQL consumers
        clickstream consumer stop --type opensearch    # Stop only OpenSearch consumer
    """
    stop_postgresql = consumer_type in (ConsumerType.all, ConsumerType.postgresql)
    stop_opensearch = consumer_type in (ConsumerType.all, ConsumerType.opensearch)

    running = count_running_consumers()
    opensearch_running = is_opensearch_consumer_running()

    # Check if requested type(s) are running
    requested_running = False
    if stop_postgresql and running > 0:
        requested_running = True
    if stop_opensearch and opensearch_running:
        requested_running = True

    if not requested_running:
        if consumer_type == ConsumerType.all:
            print(f"{C.BRIGHT_YELLOW}{I.STOP} No consumers are running{C.RESET}")
        elif consumer_type == ConsumerType.postgresql:
            print(f"{C.BRIGHT_YELLOW}{I.STOP} PostgreSQL consumers are not running{C.RESET}")
        else:
            print(f"{C.BRIGHT_YELLOW}{I.STOP} OpenSearch consumer is not running{C.RESET}")
        return

    if stop_postgresql and running > 0:
        print(f"  Stopping {running} PostgreSQL consumer instances...")
        stopped = stop_all_consumers()
        print(
            f"{C.BRIGHT_GREEN}{I.CHECK} PostgreSQL consumers stopped ({stopped} instances){C.RESET}"
        )

    if stop_opensearch and opensearch_running:
        print(f"  Stopping OpenSearch consumer...")
        stop_opensearch_consumer()
        print(f"{C.BRIGHT_GREEN}{I.CHECK} OpenSearch consumer stopped{C.RESET}")


def consumer_restart(
    truncate_log: Annotated[
        bool, typer.Option("--truncate-log", "-t", help="Truncate log files before starting")
    ] = False,
    consumer_type: Annotated[
        ConsumerType,
        typer.Option("--type", "-T", help="Which consumer(s) to restart"),
    ] = ConsumerType.all,
) -> None:
    """Restart consumer instances.

    Stops any running consumers of the specified type, then starts fresh instances.
    Useful after changing configuration (e.g., enabling OpenSearch).

    Use --type to restart only specific consumers:
    - all (default): Restart both PostgreSQL and OpenSearch consumers
    - postgresql: Restart only PostgreSQL consumers
    - opensearch: Restart only OpenSearch consumer

    Examples:
        clickstream consumer restart
        clickstream consumer restart --type postgresql     # Restart only PostgreSQL consumers
        clickstream consumer restart --type opensearch     # Restart only OpenSearch consumer
        clickstream consumer restart --truncate-log        # Clear logs before starting
    """
    restart_postgresql = consumer_type in (ConsumerType.all, ConsumerType.postgresql)
    restart_opensearch = consumer_type in (ConsumerType.all, ConsumerType.opensearch)

    # Stop if running
    running = count_running_consumers()
    opensearch_running = is_opensearch_consumer_running()

    if restart_postgresql and running > 0:
        print(f"  Stopping {running} PostgreSQL consumer instances...")
        stop_all_consumers()

    if restart_opensearch and opensearch_running:
        print(f"  Stopping OpenSearch consumer...")
        stop_opensearch_consumer()

    if (restart_postgresql and running > 0) or (restart_opensearch and opensearch_running):
        time.sleep(1)  # Brief pause to ensure clean shutdown

    # Start fresh
    consumer_start(truncate_log=truncate_log, consumer_type=consumer_type)


def consumer_logs(
    follow: Annotated[
        bool, typer.Option("--follow", "-f", help="Follow log output (like tail -f)")
    ] = False,
    lines: Annotated[int, typer.Option("--lines", "-n", help="Number of lines to show")] = 50,
) -> None:
    """View consumer log output.

    Shows an interactive menu to select which consumer log to view.
    Only non-empty log files are shown.

    Examples:
        clickstream consumer logs              # Interactive menu, show last 50 lines
        clickstream consumer logs -n 100       # Interactive menu, show last 100 lines
        clickstream consumer logs -f           # Interactive menu, then follow selected log
    """
    import glob as glob_module

    def _discover_consumer_logs() -> list[tuple[int, str, Path]]:
        """Find all non-empty consumer log files.

        Returns list of (instance, consumer_type, path) tuples sorted by instance.
        """
        logs = []
        for log_path in glob_module.glob("/tmp/consumer_*_*.log"):
            path = Path(log_path)
            # Skip empty files
            if path.stat().st_size == 0:
                continue
            # Parse filename: consumer_N_type.log
            try:
                parts = path.stem.split("_")  # ['consumer', 'N', 'type']
                instance = int(parts[1])
                consumer_type = parts[2]
                logs.append((instance, consumer_type, path))
            except (ValueError, IndexError):
                continue
        return sorted(logs, key=lambda x: (x[0], x[1]))

    # Discover available log files
    available_logs = _discover_consumer_logs()

    if not available_logs:
        print(f"{C.BRIGHT_YELLOW}{I.STOP} No consumer logs found{C.RESET}")
        print(f"  Run '{C.DIM}clickstream consumer start{C.RESET}' first")
        return

    # If only one log, show it directly
    if len(available_logs) == 1:
        instance, consumer_type, log_file = available_logs[0]
        _show_log(log_file, f"consumer_{instance}_{consumer_type}", follow, lines)
        return

    # Show interactive menu
    print()
    print(f"  {C.BOLD}Available consumer logs:{C.RESET}")
    print()
    for idx, (instance, consumer_type, path) in enumerate(available_logs):
        # Get file size for display
        size_bytes = path.stat().st_size
        if size_bytes < 1024:
            size_str = f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            size_str = f"{size_bytes // 1024} KB"
        else:
            size_str = f"{size_bytes // (1024 * 1024)} MB"
        print(f"    [{idx}] {path.name} ({size_str})")
    print()

    # Get user selection
    try:
        selection = input(f"  Enter number (or 'q' to quit): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return

    if selection == "q" or selection == "":
        return

    try:
        idx = int(selection)
        if idx < 0 or idx >= len(available_logs):
            print(f"{C.BRIGHT_RED}{I.CROSS} Invalid selection{C.RESET}")
            return
    except ValueError:
        print(f"{C.BRIGHT_RED}{I.CROSS} Invalid selection{C.RESET}")
        return

    # Show the selected log
    instance, consumer_type, log_file = available_logs[idx]
    print()
    _show_log(log_file, f"consumer_{instance}_{consumer_type}", follow, lines)
