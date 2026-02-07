# ==============================================================================
# Benchmark Command
# ==============================================================================
"""
Benchmark command for the clickstream pipeline CLI.

Measures consumer throughput from Kafka to PostgreSQL.
"""

import csv
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Optional

import typer
from rich.console import Console
from rich.table import Table

from clickstream.cli.shared import (
    C,
    I,
    PRODUCER_PID_FILE,
    count_running_consumers,
    get_all_consumer_pids,
    purge_kafka_topic,
    reset_consumer_group,
    start_consumer_instance,
    stop_all_consumers,
    get_project_root,
    is_process_running,
)
from clickstream.consumers import get_consumer
from clickstream.utils.config import get_settings

logger = logging.getLogger(__name__)


# ==============================================================================
# Helper Functions
# ==============================================================================


def _get_csv_row_count(filepath: Path) -> int:
    """Count data rows in CSV file (excludes header).

    Args:
        filepath: Path to CSV file

    Returns:
        Number of data rows (excluding header)
    """
    with open(filepath, "r") as f:
        # Count lines and subtract 1 for header
        return sum(1 for _ in f) - 1


class PgCounter:
    """Persistent PostgreSQL connection for counting events during benchmark.

    Maintains a single connection across multiple count() calls, avoiding
    the overhead and connection-leak risk of opening a new connection every
    second.  Automatically reconnects on transient errors and logs all
    failures instead of silently returning 0.

    Usage::

        with PgCounter() as pg:
            count = pg.count()          # fast: reuses connection
            count = pg.count(retries=3) # retry on failure
    """

    CONNECT_TIMEOUT = 10  # seconds – matches consumer repositories

    def __init__(self) -> None:
        import psycopg2  # noqa: F811 – keep import local so module loads without psycopg2

        self._psycopg2 = psycopg2
        self._settings = get_settings()
        self._conn: Any = None  # psycopg2 connection
        self._schema = self._settings.postgres.schema_name

    # -- context manager --------------------------------------------------

    def __enter__(self) -> "PgCounter":
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[no-untyped-def]
        self.close()
        return None

    # -- public API -------------------------------------------------------

    def count(self, *, retries: int = 0) -> int:
        """Return the current event count from PostgreSQL.

        Args:
            retries: Number of extra attempts after the first failure.
                     Each retry will reconnect before querying.

        Returns:
            Event count, or -1 if all attempts fail (never silently returns 0).
        """
        last_err: Exception | None = None
        for attempt in range(1 + retries):
            try:
                if self._conn is None or self._conn.closed:
                    self._connect()
                with self._conn.cursor() as cur:  # type: ignore[union-attr]
                    cur.execute(f"SELECT COUNT(*) FROM {self._schema}.events")
                    result = cur.fetchone()
                    return result[0] if result else 0
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "PgCounter.count() attempt %d/%d failed: %s",
                    attempt + 1,
                    1 + retries,
                    exc,
                )
                # Reconnect before next attempt
                self._safe_close()

        logger.error("PgCounter.count() failed after %d attempt(s): %s", 1 + retries, last_err)
        return -1

    def close(self) -> None:
        """Close the persistent connection."""
        self._safe_close()

    # -- internals --------------------------------------------------------

    def _connect(self) -> None:
        """Open (or reopen) the PostgreSQL connection with a timeout."""
        self._safe_close()
        s = self._settings.postgres
        self._conn = self._psycopg2.connect(
            host=s.host,
            port=s.port,
            user=s.user,
            password=s.password,
            dbname=s.database,
            sslmode=s.sslmode,
            connect_timeout=self.CONNECT_TIMEOUT,
        )
        logger.debug("PgCounter connected to %s:%s/%s", s.host, s.port, s.database)

    def _safe_close(self) -> None:
        """Close connection without raising."""
        if self._conn is not None:
            try:
                self._conn.close()  # type: ignore[union-attr]
            except Exception:
                pass
            self._conn = None


def _run_producer_blocking(limit: int, offset: int = 0) -> bool:
    """Run producer and wait for it to complete. Returns True if successful.

    Args:
        limit: Number of events to produce
        offset: Number of rows to skip in CSV (0 = start at row 1)
    """
    project_root = get_project_root()
    runner_script = project_root / "clickstream" / "producer_runner.py"

    env = os.environ.copy()
    python_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{project_root}:{python_path}" if python_path else str(project_root)
    env["PRODUCER_LIMIT"] = str(limit)
    env["PRODUCER_OFFSET"] = str(offset)

    # Run producer in foreground and wait for completion
    process = subprocess.Popen(
        [sys.executable, str(runner_script)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        cwd=str(project_root),
    )

    # Write PID file so 'clickstream status' can detect the running producer
    PRODUCER_PID_FILE.write_text(str(process.pid))

    try:
        # Wait for process to complete
        return_code = process.wait()
        return return_code == 0
    finally:
        # Clean up PID file
        PRODUCER_PID_FILE.unlink(missing_ok=True)


def _wait_for_consumers_stable(
    limit: int,
    group_id: str,
    pg: PgCounter,
    quiet: bool = False,
    stall_timeout: int = 30,
) -> tuple[int, float]:
    """
    Wait for consumers to finish processing by watching for process exit.

    In benchmark mode, consumers exit when all partitions reach EOF.
    This function monitors the consumer processes and waits for them to exit,
    then returns the final PostgreSQL event count.

    Args:
        limit: Target event count (for progress display)
        group_id: Kafka consumer group ID (unused, kept for API compatibility)
        pg: Persistent PostgreSQL counter (avoids per-call connections)
        quiet: Suppress output
        stall_timeout: Seconds of no change before considering stalled (default: 30)

    Returns:
        Tuple of (final_count, duration_seconds)
    """
    start_time = time.time()
    last_count = 0
    stall_seconds = 0
    consecutive_errors = 0

    while True:
        # Check if consumers are still running
        running_pids = get_all_consumer_pids()

        # Get current count for progress display
        count = pg.count(retries=1)

        # Track consecutive query failures (-1 means query failed)
        if count == -1:
            consecutive_errors += 1
            # Use last known good count for display
            display_count = last_count
        else:
            consecutive_errors = 0
            display_count = count

        # Calculate progress bar (round to avoid 99% when at 99.9%)
        percent = min(100, round(display_count / limit * 100)) if limit > 0 else 0
        filled = int(percent / 5)  # 20 chars total, each = 5%
        bar = "█" * filled + "░" * (20 - filled)

        # Print progress (update in place)
        if not quiet:
            error_indicator = f" {C.BRIGHT_RED}[DB error]{C.RESET}" if count == -1 else ""
            print(
                f"\r  • Waiting for consumers... [{bar}] {percent:3d}% ({display_count:,} events){error_indicator}   ",
                end="",
                flush=True,
            )

        # Primary exit: All consumers have exited (finished processing)
        if len(running_pids) == 0:
            # Final count with retries — this is the value written to CSV
            final_count = pg.count(retries=3)

            if final_count == -1:
                logger.error(
                    "Failed to get final event count from PostgreSQL after consumers exited. "
                    "Last known count: %d",
                    last_count,
                )
                # Fall back to last known good count rather than reporting 0
                final_count = last_count

            # Print final state with newline
            if not quiet:
                percent = min(100, round(final_count / limit * 100)) if limit > 0 else 100
                filled = int(percent / 5)
                bar = "█" * filled + "░" * (20 - filled)
                print(
                    f"\r  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Waiting for consumers... [{bar}] {percent:3d}%                    "
                )

            duration = time.time() - start_time
            return final_count, duration

        # Stall detection: count unchanged for stall_timeout seconds
        # Works whether count is 0 (connection failures) or a positive value
        effective_count = count if count != -1 else -1
        if effective_count == last_count and last_count >= 0:
            stall_seconds += 1
            if stall_seconds >= stall_timeout:
                if not quiet:
                    if last_count == 0:
                        print(
                            f"\n  {C.BRIGHT_RED}{I.CROSS}{C.RESET} No events received for {stall_timeout} seconds "
                            f"(possible DB connection issue — check logs)"
                        )
                    else:
                        print(
                            f"\n  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Processing stalled at {last_count:,} events for {stall_timeout} seconds"
                        )
                raise RuntimeError(
                    f"Processing stalled at {last_count:,}/{limit:,} events "
                    f"for {stall_timeout} seconds"
                )
        elif count != -1:
            # Only reset stall counter on successful queries that show progress
            stall_seconds = 0

        # Update last_count only on successful queries
        if count != -1:
            last_count = count

        time.sleep(1)


def _print_summary(results: list[tuple[int, int]], is_incremental: bool) -> None:
    """Print summary after multiple runs.

    Args:
        results: List of (events, throughput) tuples
        is_incremental: Whether runs used incrementing event counts
    """
    if not results:
        return

    throughputs = [tp for _, tp in results]

    print()
    if is_incremental:
        # Show per-run results
        first_events = results[0][0]
        last_events = results[-1][0]
        print(
            f"  {C.BOLD}Summary ({len(results)} runs from {first_events:,} to {last_events:,} events){C.RESET}"
        )
        for events, tp in results:
            print(f"  • {events:>10,} events: {_format_abbreviated(tp)} events/sec")
    else:
        events = results[0][0]
        print(f"  {C.BOLD}Summary ({len(results)} runs at {events:,} events){C.RESET}")

    # Always show aggregate stats
    print(
        f"  {C.BOLD}Throughput:{C.RESET} "
        f"min {_format_abbreviated(min(throughputs))}, "
        f"max {_format_abbreviated(max(throughputs))}, "
        f"avg {_format_abbreviated(sum(throughputs) // len(throughputs))} events/sec"
    )


# ==============================================================================
# Commands
# ==============================================================================


def benchmark_run(
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", "-l", help="Number of events to produce (default: all)"),
    ] = None,
    output: Annotated[Path, typer.Option("--output", "-o", help="Output CSV file")] = Path(
        "benchmark_results.csv"
    ),
    confirm: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
    network: Annotated[
        bool,
        typer.Option(
            "--network", "-n", help="Include full network measurements (latency + speedtest)"
        ),
    ] = False,
    network_latency: Annotated[
        bool,
        typer.Option("--network-latency", help="Include service latency measurements only"),
    ] = False,
    runs: Annotated[int, typer.Option("--runs", "-r", help="Number of benchmark runs")] = 1,
    increment: Annotated[
        int, typer.Option("--increment", "-i", help="Increase event count by this amount each run")
    ] = 0,
    quiet: Annotated[
        bool, typer.Option("--quiet", "-q", help="Suppress output for programmatic use")
    ] = False,
    offset: Annotated[
        int, typer.Option("--offset", help="Number of rows to skip in CSV (0 = start at row 1)")
    ] = 0,
    impl: Annotated[
        Optional[str],
        typer.Option(
            "--consumer-impl",
            help="Override consumer implementation (confluent, kafka_python, quix, mage, bytewax)",
        ),
    ] = None,
) -> None:
    """Run a benchmark measuring consumer throughput.

    Measures how quickly consumers process events from Kafka to PostgreSQL.
    The number of consumers equals KAFKA_EVENTS_TOPIC_PARTITIONS.

    Before running, ensure KAFKA_NUM_PARTITIONS in docker-compose.yml matches
    KAFKA_EVENTS_TOPIC_PARTITIONS in your .env file.

    Examples:
        clickstream benchmark run -y                                       # All events
        clickstream benchmark run --limit 100000 -y                        # Specific limit
        clickstream benchmark run --limit 100000 --runs 5 -y               # 5 runs for averaging
        clickstream benchmark run --limit 100000 --increment 50000 --runs 5 -y  # Incremental
        clickstream benchmark run --offset 10000 --limit 5000 -y           # Skip first 10000 rows
        clickstream benchmark run --limit 100000 --consumer-impl quix -y    # Use specific consumer
    """
    from clickstream.utils.db import reset_schema
    from clickstream.utils.session_state import get_valkey_client

    # Helper for conditional printing
    def _print(msg: str = "") -> None:
        if not quiet:
            print(msg)

    # Validate options
    if runs < 1:
        print(f"{C.BRIGHT_RED}{I.CROSS} --runs must be at least 1{C.RESET}")
        raise typer.Exit(1)

    if increment > 0 and runs <= 1:
        print(f"{C.BRIGHT_RED}{I.CROSS} --increment requires --runs > 1{C.RESET}")
        raise typer.Exit(1)

    if increment < 0:
        print(f"{C.BRIGHT_RED}{I.CROSS} --increment must be positive{C.RESET}")
        raise typer.Exit(1)

    if offset < 0:
        print(f"{C.BRIGHT_RED}{I.CROSS} --offset must be non-negative{C.RESET}")
        raise typer.Exit(1)

    if limit is None and increment > 0:
        print(f"{C.BRIGHT_RED}{I.CROSS} --increment requires --limit{C.RESET}")
        raise typer.Exit(1)

    # Validate --impl option
    valid_impls = ["confluent", "kafka_python", "quix", "mage", "bytewax"]
    impl_packages = {
        "confluent": "confluent-kafka",
        "kafka_python": "kafka-python",
        "quix": "quixstreams",
        "mage": "mage-ai",
        "bytewax": "bytewax",
    }
    if impl is not None:
        if impl not in valid_impls:
            print(
                f"{C.BRIGHT_RED}{I.CROSS} Invalid --impl value: {impl}. "
                f"Valid options: {', '.join(valid_impls)}{C.RESET}"
            )
            raise typer.Exit(1)
        # Check that the package is installed
        from importlib.metadata import PackageNotFoundError, version as pkg_version

        package_name = impl_packages[impl]
        try:
            pkg_version(package_name)
        except PackageNotFoundError:
            print(
                f"{C.BRIGHT_RED}{I.CROSS} Package '{package_name}' is not installed. "
                f"Install it to use --impl {impl}{C.RESET}"
            )
            raise typer.Exit(1)

    # Count CSV rows once for validation
    events_file = get_project_root() / "data" / "events.csv"
    if not events_file.exists():
        print(f"{C.BRIGHT_RED}{I.CROSS} Events file not found: {events_file}{C.RESET}")
        raise typer.Exit(1)

    total_rows = _get_csv_row_count(events_file)

    # Default to all available rows (accounting for offset)
    if limit is None:
        limit = total_rows - offset

    if offset >= total_rows:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} --offset ({offset:,}) exceeds available rows ({total_rows:,}){C.RESET}"
        )
        raise typer.Exit(1)

    # Calculate final limit (for last run with increment)
    final_limit = limit + (increment * (runs - 1))
    if offset + final_limit > total_rows:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Final run would exceed available rows "
            f"(need {offset + final_limit:,}, have {total_rows:,}){C.RESET}"
        )
        raise typer.Exit(1)

    # --quiet implies --yes
    if quiet:
        confirm = True

    settings = get_settings()
    partitions = settings.kafka.events_topic_partitions

    # Determine effective consumer impl (override or default)
    effective_impl = impl if impl is not None else settings.consumer.impl

    # Get consumer with the effective impl
    # Note: We need to temporarily set the env var for get_consumer to pick it up
    import os

    from clickstream.utils.config import get_settings as _get_settings

    original_impl = os.environ.get("CONSUMER_IMPL")
    if impl is not None:
        os.environ["CONSUMER_IMPL"] = impl
        # Clear the settings cache to pick up the new value
        _get_settings.cache_clear()

    try:
        consumer = get_consumer("postgresql")
        num_instances = consumer.num_instances
    finally:
        # Restore original env var
        if impl is not None:
            if original_impl is not None:
                os.environ["CONSUMER_IMPL"] = original_impl
            else:
                os.environ.pop("CONSUMER_IMPL", None)
            _get_settings.cache_clear()

    # Configuration display
    _print()
    _print(f"  {C.BOLD}Benchmark Configuration{C.RESET}")
    if impl is not None and impl != settings.consumer.impl:
        _print(
            f"  • Consumer Impl:        {C.WHITE}{effective_impl}{C.RESET} {C.BRIGHT_YELLOW}(override){C.RESET}"
        )
    else:
        _print(f"  • Consumer Impl:        {C.WHITE}{effective_impl}{C.RESET}")
    _print(f"  • Partitions:           {C.WHITE}{partitions}{C.RESET}")
    _print(f"  • Consumers:            {C.WHITE}{consumer.parallelism_description}{C.RESET}")
    if offset > 0:
        _print(f"  • Offset (rows to skip): {C.WHITE}{offset:,}{C.RESET}")
    if increment > 0:
        _print(f"  • Starting events:      {C.WHITE}{limit:,}{C.RESET}")
        _print(f"  • Increment:            {C.WHITE}{increment:,}{C.RESET}")
    else:
        _print(f"  • Events per run:       {C.WHITE}{limit:,}{C.RESET}")
    if runs > 1:
        _print(f"  • Runs:                 {C.WHITE}{runs}{C.RESET}")
    _print(f"  • Output file:          {C.WHITE}{output}{C.RESET}")
    _print()

    # Confirmation prompt
    if not confirm:
        print(
            f"{C.BRIGHT_YELLOW}{I.WARN} This will reset all data before each benchmark run.{C.RESET}"
        )
        typer.confirm("Continue?", abort=True)
        _print()

    # Pre-flight checks (always run, even in quiet mode)
    _print(f"  {C.BOLD}Pre-flight checks{C.RESET}")

    # Check consumer is stopped
    running_consumers = count_running_consumers()
    if running_consumers > 0:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Consumer is running ({running_consumers} instances) - "
            f"stop it first with '{C.WHITE}clickstream consumer stop{C.BRIGHT_RED}'{C.RESET}"
        )
        raise typer.Exit(1)
    _print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Consumer is stopped")

    # Check producer is stopped
    if is_process_running(PRODUCER_PID_FILE):
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Producer is running - "
            f"stop it first with '{C.WHITE}clickstream producer stop{C.BRIGHT_RED}'{C.RESET}"
        )
        raise typer.Exit(1)
    _print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Producer is stopped")

    # Check services
    from clickstream.utils.config import get_health_checker

    health = get_health_checker(settings)
    required = ["kafka", "pg", "valkey"]
    svc_labels = {"kafka": "Kafka", "pg": "PostgreSQL", "valkey": "Valkey"}

    first_check = True

    def _on_status(statuses):
        nonlocal first_check
        if first_check:
            first_check = False
            all_running = all(s.is_running for s in statuses.values())
            if not all_running:
                _print(f"  {C.BRIGHT_YELLOW}{I.WARN}{C.RESET} Waiting for services...")
                for svc, status in statuses.items():
                    icon = (
                        f"{C.BRIGHT_GREEN}{I.CHECK}"
                        if status.is_running
                        else f"{C.BRIGHT_YELLOW}{I.WARN}"
                    )
                    _print(f"    {icon}{C.RESET} {svc_labels.get(svc, svc)}: {status.state}")

    try:
        health.wait_until_ready(required, timeout=300.0, poll_interval=10.0, on_status=_on_status)
        for svc in required:
            _print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} {svc_labels[svc]} is reachable")
    except TimeoutError as e:
        print(f"{C.BRIGHT_RED}{I.CROSS} {e}{C.RESET}")
        raise typer.Exit(1)

    # Track results for summary
    results: list[tuple[int, int]] = []  # (events, throughput)

    # Run benchmark loop
    for run_num in range(1, runs + 1):
        current_limit = limit + (increment * (run_num - 1))

        # Run header
        _print()
        if runs > 1:
            if increment > 0:
                _print(f"  {C.BOLD}Run {run_num}/{runs} ({current_limit:,} events){C.RESET}")
            else:
                _print(f"  {C.BOLD}Run {run_num}/{runs}{C.RESET}")
        else:
            _print(f"  {C.BOLD}Running benchmark{C.RESET}")

        try:
            # Open a persistent PostgreSQL connection for this run
            with PgCounter() as pg:
                # Network measurements (if requested) - re-measure each run
                network_metrics = None
                if network or network_latency:
                    from clickstream.utils.network import collect_network_metrics, measure_latencies

                    if network:
                        network_metrics = collect_network_metrics(settings)
                    else:
                        network_metrics = measure_latencies(settings)

                # Reset data
                topic = settings.kafka.events_topic
                purge_kafka_topic(topic)
                reset_schema()
                client = get_valkey_client()
                client.flushall()
                consumer_group = settings.postgresql_consumer.group_id
                reset_consumer_group(consumer_group)
                _print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Resetting data")

                # Reconnect after schema reset (the old connection's table was dropped)
                pg.close()
                pg._connect()

                # Run producer first (so consumers have data to process)
                producer_success = _run_producer_blocking(current_limit, offset)
                if not producer_success:
                    _print(
                        f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Running producer ({current_limit:,} events)"
                    )
                    if not quiet:
                        print(
                            f"  {C.BRIGHT_YELLOW}{I.WARN} Run {run_num} failed, continuing...{C.RESET}"
                        )
                    continue

                _print(
                    f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Running producer ({current_limit:,} events)"
                )

                # Start consumers (with benchmark mode for EOF exit)
                project_root = get_project_root()
                runner_script = project_root / "clickstream" / "consumer_runner.py"

                for i in range(num_instances):
                    start_consumer_instance(
                        runner_script, i, project_root, benchmark_mode=True, impl_override=impl
                    )
                    if i < num_instances - 1:
                        time.sleep(1)

                time.sleep(2)

                running = get_all_consumer_pids()
                if len(running) != num_instances:
                    _print(
                        f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Starting consumer ({consumer.parallelism_description})"
                    )
                    _print(
                        f"    {C.BRIGHT_RED}Only {len(running)}/{num_instances} processes started{C.RESET}"
                    )
                    stop_all_consumers()
                    if not quiet:
                        print(
                            f"  {C.BRIGHT_YELLOW}{I.WARN} Run {run_num} failed, continuing...{C.RESET}"
                        )
                    continue

                _print(
                    f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Starting consumer ({consumer.parallelism_description})"
                )

                # Wait for consumers (they will exit when EOF is reached)
                consumer_group = settings.postgresql_consumer.group_id
                events_consumed, duration = _wait_for_consumers_stable(
                    current_limit, consumer_group, pg=pg, quiet=quiet
                )

                # Consumers have exited on their own (benchmark mode)
                # Clean up any remaining PID files
                stop_all_consumers()
                _print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Consumers finished")

            # Calculate throughput
            throughput = round(events_consumed / duration) if duration > 0 else 0

            # Show per-run result
            _print(f"  • Throughput: {_format_abbreviated(throughput)} events/sec")

            # Record result
            results.append((current_limit, throughput))

            # Write to CSV
            timestamp = datetime.now().isoformat(timespec="seconds")
            environment = settings.detect_environment()
            consumer_name = consumer.name

            base_header = "timestamp,environment,consumer_impl,partitions,events_produced,events_consumed,duration_sec,throughput_events_sec"
            base_row = f"{timestamp},{environment},{consumer_name},{partitions},{current_limit},{events_consumed},{round(duration)},{throughput}"

            if network and network_metrics:
                csv_header = f"{base_header},kafka_latency_ms,pg_latency_ms,valkey_latency_ms,upload_mbps,ping_ms\n"
                csv_row = (
                    f"{base_row},"
                    f"{network_metrics.kafka_latency_ms or ''},"
                    f"{network_metrics.pg_latency_ms or ''},"
                    f"{network_metrics.valkey_latency_ms or ''},"
                    f"{network_metrics.upload_mbps or ''},"
                    f"{network_metrics.ping_ms or ''}\n"
                )
            elif network_latency and network_metrics:
                csv_header = f"{base_header},kafka_latency_ms,pg_latency_ms,valkey_latency_ms\n"
                csv_row = (
                    f"{base_row},"
                    f"{network_metrics.kafka_latency_ms or ''},"
                    f"{network_metrics.pg_latency_ms or ''},"
                    f"{network_metrics.valkey_latency_ms or ''}\n"
                )
            else:
                csv_header = f"{base_header}\n"
                csv_row = f"{base_row}\n"

            write_header = not output.exists()
            with open(output, "a") as f:
                if write_header:
                    f.write(csv_header)
                f.write(csv_row)

        except KeyboardInterrupt:
            stop_all_consumers()
            if not quiet:
                print(f"\n  {C.BRIGHT_RED}{I.CROSS} Run {run_num} interrupted{C.RESET}")
            break
        except Exception as e:
            stop_all_consumers()
            if not quiet:
                print(f"  {C.BRIGHT_RED}{I.CROSS} Run {run_num} failed: {e}{C.RESET}")
            continue

    # Print summary for multi-run
    if runs > 1 and results and not quiet:
        _print_summary(results, increment > 0)

    _print()
    if results:
        _print(f"{C.BRIGHT_GREEN}{I.CHECK} Results appended to {output}{C.RESET}")
    else:
        _print(f"{C.BRIGHT_RED}{I.CROSS} All runs failed{C.RESET}")
    _print()


def _parse_date(date_str: str) -> datetime:
    """Parse ISO date or relative date (7d, 2w, 1m) to datetime.

    Args:
        date_str: Date string in ISO format (2026-01-25) or relative format (7d, 2w, 1m)

    Returns:
        datetime object

    Raises:
        typer.BadParameter: If the date format is invalid
    """
    # Try relative date format: Nd, Nw, Nm
    match = re.match(r"^(\d+)([dwm])$", date_str.lower())
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        now = datetime.now()
        if unit == "d":
            return now - timedelta(days=value)
        elif unit == "w":
            return now - timedelta(weeks=value)
        elif unit == "m":
            # Approximate month as 30 days
            return now - timedelta(days=value * 30)

    # Try ISO date formats
    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    raise typer.BadParameter(
        f"Invalid date format: {date_str}. Use ISO date (2026-01-25) or relative (7d, 2w, 1m)"
    )


def _format_abbreviated(value: int) -> str:
    """Format number with K/M/G suffix (e.g., 8783 -> 8.8K)."""
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}G"
    elif value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}K"
    else:
        return str(value)


def _format_duration(seconds: int) -> str:
    """Format seconds as 'Xm Ys' or 'Ys' for durations under 60s."""
    if seconds >= 60:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs:02d}s"
    else:
        return f"{seconds}s"


def benchmark_show(
    file: Annotated[Path, typer.Option("--file", "-f", help="Benchmark results CSV file")] = Path(
        "benchmark_results.csv"
    ),
    environment: Annotated[
        Optional[str],
        typer.Option("--environment", "-e", help="Filter by environment (local/aiven)"),
    ] = None,
    since: Annotated[
        Optional[str],
        typer.Option("--since", help="Show results since date (ISO or relative: 7d, 2w, 1m)"),
    ] = None,
    until: Annotated[
        Optional[str], typer.Option("--until", help="Show results until date (ISO format)")
    ] = None,
    summary: Annotated[
        bool, typer.Option("--summary", "-s", help="Show summary statistics")
    ] = False,
) -> None:
    """Show benchmark results from CSV file.

    Displays benchmark results in a formatted table with optional filtering
    by environment and date range.

    Examples:
        clickstream benchmark show                        # Show all results
        clickstream benchmark show --summary              # Include summary statistics
        clickstream benchmark show -e aiven               # Filter to Aiven only
        clickstream benchmark show --since 7d             # Last 7 days
        clickstream benchmark show --since 2026-01-25 -s  # Since date with summary
        clickstream benchmark show -f custom.csv          # Custom file
    """
    # Validate environment option
    if environment is not None and environment not in ("local", "aiven"):
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Invalid environment: {environment}. Use 'local' or 'aiven'{C.RESET}"
        )
        raise typer.Exit(1)

    # Check file exists
    if not file.exists():
        print(f"{C.BRIGHT_RED}{I.CROSS} No benchmark results found at {file}{C.RESET}")
        raise typer.Exit(1)

    # Parse date filters
    since_dt: Optional[datetime] = None
    until_dt: Optional[datetime] = None
    if since:
        since_dt = _parse_date(since)
    if until:
        until_dt = _parse_date(until)

    # Read CSV file
    with open(file, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            print(f"{C.BRIGHT_RED}{I.CROSS} No benchmark data in {file}{C.RESET}")
            raise typer.Exit(1)

        headers = list(reader.fieldnames)
        rows = list(reader)

    if not rows:
        print(f"{C.BRIGHT_RED}{I.CROSS} No benchmark data in {file}{C.RESET}")
        raise typer.Exit(1)

    # Filter rows
    filtered_rows = []
    for row in rows:
        # Parse timestamp
        try:
            row_dt = datetime.fromisoformat(row["timestamp"])
        except (ValueError, KeyError):
            continue

        # Apply filters
        if environment and row.get("environment") != environment:
            continue
        if since_dt and row_dt < since_dt:
            continue
        if until_dt and row_dt > until_dt:
            continue

        filtered_rows.append(row)

    if not filtered_rows:
        print(f"{C.BRIGHT_YELLOW}{I.WARN} No results match the specified filters{C.RESET}")
        raise typer.Exit(0)

    # Build table
    console = Console()
    table = Table(title=f"Benchmark Results ({file})", show_header=True, header_style="bold")

    # Column configuration: (csv_name, display_name, justify, formatter)
    column_config = [
        ("timestamp", "Timestamp", "left", lambda v: v[:16].replace("T", " ") if v else ""),
        ("environment", "Env", "left", lambda v: v or ""),
        ("consumer_impl", "Consumer Impl", "left", lambda v: v or ""),
        ("partitions", "Parts", "right", lambda v: v or ""),
        ("events_produced", "Produced", "right", lambda v: f"{int(v):,}" if v else ""),
        ("events_consumed", "Consumed", "right", lambda v: f"{int(v):,}" if v else ""),
        ("duration_sec", "Duration", "right", lambda v: _format_duration(int(v)) if v else ""),
        (
            "throughput_events_sec",
            "Throughput",
            "right",
            lambda v: f"{_format_abbreviated(int(v))}/s" if v else "",
        ),
        # Optional network columns
        ("kafka_latency_ms", "Kafka", "right", lambda v: f"{v} ms" if v else ""),
        ("pg_latency_ms", "PG", "right", lambda v: f"{v} ms" if v else ""),
        ("valkey_latency_ms", "Valkey", "right", lambda v: f"{v} ms" if v else ""),
        ("upload_mbps", "Upload", "right", lambda v: f"{v} Mbps" if v else ""),
        ("ping_ms", "Ping", "right", lambda v: f"{v} ms" if v else ""),
    ]

    # Add columns that exist in the CSV
    active_columns = []
    for csv_name, display_name, justify, formatter in column_config:
        if csv_name in headers:
            table.add_column(display_name, justify=justify)
            active_columns.append((csv_name, formatter))

    # Add rows
    for row in filtered_rows:
        table.add_row(*[formatter(row.get(csv_name, "")) for csv_name, formatter in active_columns])

    print()
    console.print(table)

    # Summary statistics
    if summary:
        throughputs = []
        environments: dict[str, list[int]] = {}

        for row in filtered_rows:
            try:
                tp = int(row["throughput_events_sec"])
                throughputs.append(tp)
                env = row.get("environment", "unknown")
                if env not in environments:
                    environments[env] = []
                environments[env].append(tp)
            except (ValueError, KeyError):
                continue

        if throughputs:
            # Date range
            timestamps = [row["timestamp"][:10] for row in filtered_rows]
            first_date = min(timestamps)
            last_date = max(timestamps)

            print()
            print(
                f"  {C.BOLD}Summary:{C.RESET} {len(filtered_rows)} runs from {first_date} to {last_date}"
            )

            # Overall stats or per-environment if multiple
            if len(environments) > 1:
                print()
                for env in sorted(environments.keys()):
                    env_tps = environments[env]
                    print(
                        f"    {env} ({len(env_tps)} runs): "
                        f"min {_format_abbreviated(min(env_tps))}, max {_format_abbreviated(max(env_tps))}, "
                        f"avg {_format_abbreviated(sum(env_tps) // len(env_tps))} events/sec"
                    )
            else:
                print(
                    f"  {C.BOLD}Throughput:{C.RESET} "
                    f"min {_format_abbreviated(min(throughputs))}, max {_format_abbreviated(max(throughputs))}, "
                    f"avg {_format_abbreviated(sum(throughputs) // len(throughputs))} events/sec"
                )

    print()
