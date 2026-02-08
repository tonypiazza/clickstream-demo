# ==============================================================================
# Producer Commands
# ==============================================================================
"""
Producer commands for the clickstream pipeline CLI.

Commands for starting, stopping, and viewing logs for the Kafka producer.
"""

from typing import Annotated

import typer

from clickstream.cli.shared import (
    PRODUCER_LOG_FILE,
    PRODUCER_PID_FILE,
    C,
    I,
    get_process_pid,
    get_project_root,
    is_process_running,
    start_background_process,
    stop_process,
)

# ==============================================================================
# Helper Functions
# ==============================================================================


def _parse_realtime_speed(value: str) -> int:
    """Parse realtime speed value like '10', '10x', or '10X' into integer."""
    value = value.strip().lower().rstrip("x")
    try:
        speed = int(value)
        if speed < 1:
            raise typer.BadParameter("Speed must be a positive integer (e.g., 1, 2, 10)")
        return speed
    except ValueError:
        raise typer.BadParameter(f"Invalid speed '{value}'. Use integer like 2, 10, or 10x")


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


def producer_list() -> None:
    """List all available producer implementations.

    Shows all producer options with version info and indicates which one
    is currently active.

    Examples:
        clickstream producer list
    """
    from clickstream.utils.config import get_settings

    settings = get_settings()
    current_impl = settings.producer.impl

    # Define available producers: (impl_key, package_name, description)
    producers = [
        ("confluent", "confluent-kafka", "librdkafka C library (fastest)"),
        ("kafka_python", "kafka-python", "Pure Python implementation"),
        ("quix", "quixstreams", "Quix Streams producer API"),
    ]

    print()
    print(f"  {C.BOLD}Available Producers:{C.RESET}")
    print()

    for impl, package_name, description in producers:
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
    print(f"  {C.DIM}Set via:{C.RESET} PRODUCER_IMPL environment variable")
    print()


def producer_start(
    realtime: Annotated[
        str | None,
        typer.Option(
            "--realtime",
            "-r",
            help="Real-time replay with optional speed (e.g., --realtime, --realtime 10x)",
        ),
    ] = None,
    limit: Annotated[
        int | None, typer.Option("--limit", "-l", help="Limit number of events (default: all)")
    ] = None,
    rate: Annotated[
        float | None,
        typer.Option("--rate", help="Target events per second (sustained rate via token bucket)"),
    ] = None,
    truncate_log: Annotated[
        bool, typer.Option("--truncate-log", "-t", help="Truncate log file before starting")
    ] = False,
) -> None:
    """Start the producer pipeline as a background process.

    Reads events from the CSV file and publishes them to Kafka.
    Runs until all events are published or stopped with 'clickstream producer stop'.

    By default, runs in batch mode (no delays) for fastest processing.
    Use --realtime to replay events with original timing.
    Use --rate to produce at a sustained events/sec rate.

    Examples:
        clickstream producer start                    # Batch mode (fastest)
        clickstream producer start --limit 1000       # Batch mode, first 1000 events
        clickstream producer start --realtime         # Real-time replay (1x speed)
        clickstream producer start --realtime 10x     # 10x faster than real-time
        clickstream producer start --rate 5000        # 5,000 events/sec sustained
        clickstream producer start --truncate-log     # Clear log before starting
    """
    # Check if already running
    if is_process_running(PRODUCER_PID_FILE):
        print(f"{C.BRIGHT_YELLOW}{I.STOP} Producer is already running{C.RESET}")
        print(f"  PID: {C.WHITE}{get_process_pid(PRODUCER_PID_FILE)}{C.RESET}")
        print(f"  Use '{C.DIM}clickstream producer stop{C.RESET}' to stop it")
        raise typer.Exit(1)

    # Validate mutual exclusivity: --rate and --realtime cannot be combined
    realtime_mode = realtime is not None
    if rate is not None and realtime_mode:
        print(f"{C.BRIGHT_RED}{I.STOP} --rate and --realtime are mutually exclusive{C.RESET}")
        print(
            f"  {C.DIM}Use --rate for sustained throughput or --realtime for replay timing{C.RESET}"
        )
        raise typer.Exit(1)

    # Truncate log file if requested
    if truncate_log and PRODUCER_LOG_FILE.exists():
        PRODUCER_LOG_FILE.unlink()
        print("  Log file truncated")

    # Parse realtime mode
    # realtime=None means batch mode, realtime="" (flag with no value) means 1x speed
    speed = 1
    if realtime_mode and realtime:
        speed = _parse_realtime_speed(realtime)

    print("  Starting producer pipeline...")
    if rate is not None:
        print(f"  Mode:  {C.WHITE}Rate-limited ({rate:,.0f} events/sec){C.RESET}")
    elif realtime_mode:
        print(f"  Mode:  {C.WHITE}Real-time ({speed}x){C.RESET}")
    else:
        print(f"  Mode:  {C.WHITE}Batch (no delays){C.RESET}")
    if limit:
        print(f"  Limit: {C.WHITE}{limit:,} events{C.RESET}")
    else:
        print(f"  Limit: {C.WHITE}All events{C.RESET}")

    # Get paths
    project_root = get_project_root()
    runner_script = project_root / "clickstream" / "producer_runner.py"

    # Pass options via environment variables
    # PRODUCER_SPEED is only set for realtime mode (presence implies realtime)
    extra_env = {}
    if realtime_mode:
        extra_env["PRODUCER_SPEED"] = str(speed)
    if limit:
        extra_env["PRODUCER_LIMIT"] = str(limit)
    if rate is not None:
        extra_env["PRODUCER_RATE"] = str(rate)

    if start_background_process(
        runner_script, PRODUCER_PID_FILE, PRODUCER_LOG_FILE, "Producer", extra_env
    ):
        print()
        print(f"  Use '{C.DIM}clickstream producer stop{C.RESET}' to stop")
        print(f"  Use '{C.DIM}clickstream producer logs{C.RESET}' to view output")
    else:
        raise typer.Exit(1)


def producer_stop() -> None:
    """Stop the running producer pipeline.

    Examples:
        clickstream producer stop
    """
    if not stop_process(PRODUCER_PID_FILE, "Producer"):
        return


def producer_logs(
    follow: Annotated[
        bool, typer.Option("--follow", "-f", help="Follow log output (like tail -f)")
    ] = False,
    lines: Annotated[int, typer.Option("--lines", "-n", help="Number of lines to show")] = 50,
) -> None:
    """View producer log output.

    Shows the most recent log entries from the producer pipeline.
    Use --follow to continuously stream new log entries.

    Examples:
        clickstream producer logs              # Show last 50 lines
        clickstream producer logs -n 100       # Show last 100 lines
        clickstream producer logs -f           # Follow log output (Ctrl+C to stop)
    """
    if not PRODUCER_LOG_FILE.exists():
        print(f"{C.BRIGHT_YELLOW}{I.STOP} No producer log file found{C.RESET}")
        print(f"  Run '{C.DIM}clickstream producer start{C.RESET}' first")
        return

    if follow:
        import subprocess

        print(f"{C.DIM}Following {PRODUCER_LOG_FILE} (Ctrl+C to stop)...{C.RESET}")
        print()
        try:
            subprocess.run(["tail", "-f", str(PRODUCER_LOG_FILE)], check=False)
        except KeyboardInterrupt:
            print()
    else:
        # Read last N lines
        with open(PRODUCER_LOG_FILE) as f:
            all_lines = f.readlines()

        if not all_lines:
            print(f"{C.DIM}Log file is empty{C.RESET}")
            return

        display_lines = all_lines[-lines:]
        print(f"{C.DIM}=== {PRODUCER_LOG_FILE} (last {len(display_lines)} lines) ==={C.RESET}")
        print()
        for line in display_lines:
            print(line, end="")
