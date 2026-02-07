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
        "benchmark_run_results.csv"
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
    file: Annotated[
        Optional[Path], typer.Option("--file", "-f", help="Benchmark results CSV file")
    ] = None,
    ramp: Annotated[bool, typer.Option("--ramp", help="Show ramp test results")] = False,
    run: Annotated[bool, typer.Option("--run", help="Show run test results (default)")] = False,
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
    by environment and date range. Use --ramp to show ramp test results or
    --run (default) for standard benchmark run results.

    Examples:
        clickstream benchmark show                        # Show run results (default)
        clickstream benchmark show --ramp                 # Show ramp results
        clickstream benchmark show --summary              # Include summary statistics
        clickstream benchmark show -e aiven               # Filter to Aiven only
        clickstream benchmark show --since 7d             # Last 7 days
        clickstream benchmark show --since 2026-01-25 -s  # Since date with summary
        clickstream benchmark show -f custom.csv          # Custom file
    """
    # Validate mutual exclusivity
    if ramp and run:
        print(f"{C.BRIGHT_RED}{I.CROSS} --ramp and --run are mutually exclusive{C.RESET}")
        raise typer.Exit(1)

    # Default to --run if neither specified
    show_ramp = ramp

    # Determine default file based on mode
    if file is None:
        file = (
            Path("benchmark_ramp_results.csv") if show_ramp else Path("benchmark_run_results.csv")
        )

    if show_ramp:
        _show_ramp_results(file, since, until, summary)
    else:
        _show_run_results(file, environment, since, until, summary)


def _show_run_results(
    file: Path,
    environment: Optional[str],
    since: Optional[str],
    until: Optional[str],
    summary: bool,
) -> None:
    """Display benchmark run results (original format)."""
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


def _show_ramp_results(
    file: Path,
    since: Optional[str],
    until: Optional[str],
    summary: bool,
) -> None:
    """Display benchmark ramp results with per-step aggregation and saturation analysis."""
    # Check file exists
    if not file.exists():
        print(f"{C.BRIGHT_RED}{I.CROSS} No ramp results found at {file}{C.RESET}")
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
            print(f"{C.BRIGHT_RED}{I.CROSS} No ramp data in {file}{C.RESET}")
            raise typer.Exit(1)

        headers = list(reader.fieldnames)
        rows = list(reader)

    if not rows:
        print(f"{C.BRIGHT_RED}{I.CROSS} No ramp data in {file}{C.RESET}")
        raise typer.Exit(1)

    # Filter rows by date
    filtered_rows = []
    for row in rows:
        try:
            row_dt = datetime.fromisoformat(row["timestamp"])
        except (ValueError, KeyError):
            continue
        if since_dt and row_dt < since_dt:
            continue
        if until_dt and row_dt > until_dt:
            continue
        filtered_rows.append(row)

    if not filtered_rows:
        print(f"{C.BRIGHT_YELLOW}{I.WARN} No results match the specified filters{C.RESET}")
        raise typer.Exit(0)

    # Aggregate samples per step
    steps: dict[int, dict] = {}
    for row in filtered_rows:
        try:
            step_num = int(row["step"])
            rate_target = int(row["rate_target"])
            throughput = int(row["consumer_throughput"])
            total_lag = int(row["total_lag"])
            trend = row.get("lag_trend", "unknown")
        except (ValueError, KeyError):
            continue

        if step_num not in steps:
            steps[step_num] = {
                "rate_target": rate_target,
                "throughputs": [],
                "lags": [],
                "trends": [],
                "sample_count": 0,
            }

        steps[step_num]["throughputs"].append(throughput)
        steps[step_num]["lags"].append(total_lag)
        steps[step_num]["trends"].append(trend)
        steps[step_num]["sample_count"] += 1

    if not steps:
        print(f"{C.BRIGHT_RED}{I.CROSS} No valid ramp data in {file}{C.RESET}")
        raise typer.Exit(1)

    # Build table
    console = Console()
    table = Table(title=f"Ramp Results ({file})", show_header=True, header_style="bold")
    table.add_column("Step", justify="right")
    table.add_column("Rate Target", justify="right")
    table.add_column("Avg Throughput", justify="right")
    table.add_column("Final Lag", justify="right")
    table.add_column("Trend", justify="left")
    table.add_column("Samples", justify="right")

    # Detect saturation point: first step where dominant trend is "growing"
    saturation_rate: Optional[int] = None
    peak_sustainable_rate: Optional[int] = None

    trend_icons = {
        "growing": "[bright_red]↑ growing[/]",
        "stable": "[bright_green]→ stable[/]",
        "shrinking": "[bright_cyan]↓ shrinking[/]",
        "unknown": "[dim]? unknown[/]",
    }

    for step_num in sorted(steps):
        step_data = steps[step_num]
        rate_target = step_data["rate_target"]
        avg_tp = sum(step_data["throughputs"]) // len(step_data["throughputs"])
        final_lag = step_data["lags"][-1]
        # Dominant trend = most common in the step's samples
        trend_counts: dict[str, int] = {}
        for t in step_data["trends"]:
            trend_counts[t] = trend_counts.get(t, 0) + 1
        dominant_trend = max(trend_counts, key=lambda k: trend_counts[k])

        # Track saturation
        if dominant_trend == "growing" and saturation_rate is None:
            saturation_rate = rate_target
        if dominant_trend in ("stable", "shrinking"):
            peak_sustainable_rate = rate_target

        table.add_row(
            str(step_num),
            f"{_format_abbreviated(rate_target)}/s",
            f"{_format_abbreviated(avg_tp)}/s",
            f"{final_lag:,}",
            trend_icons.get(dominant_trend, dominant_trend),
            str(step_data["sample_count"]),
        )

    print()
    console.print(table)

    # Summary
    if summary or True:  # Always show saturation analysis for ramp
        print()
        if saturation_rate is not None:
            print(
                f"  {C.BOLD}Saturation point:{C.RESET}  "
                f"{C.BRIGHT_YELLOW}{saturation_rate:,} events/sec{C.RESET}"
            )
        else:
            print(
                f"  {C.BOLD}Saturation point:{C.RESET}  "
                f"{C.BRIGHT_GREEN}Not reached within test range{C.RESET}"
            )

        if peak_sustainable_rate is not None:
            print(
                f"  {C.BOLD}Peak sustainable:{C.RESET}   "
                f"{C.WHITE}{peak_sustainable_rate:,} events/sec{C.RESET}"
            )

        print(f"  {C.BOLD}Total samples:{C.RESET}     {len(filtered_rows)}")
        print(f"  {C.BOLD}Steps completed:{C.RESET}   {len(steps)}")

    print()


# ==============================================================================
# Ramp Test Helpers
# ==============================================================================


RAMP_SAMPLE_INTERVAL = 5  # seconds between lag/throughput samples


def _run_producer_background(rate: float) -> subprocess.Popen:
    """Start a rate-limited producer as a background subprocess.

    The producer runs indefinitely (no PRODUCER_LIMIT) until explicitly stopped.
    Uses PRODUCER_RATE env var to set the token bucket rate.

    Args:
        rate: Target events per second

    Returns:
        The Popen handle for the running producer process
    """
    project_root = get_project_root()
    runner_script = project_root / "clickstream" / "producer_runner.py"

    env = os.environ.copy()
    python_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{project_root}:{python_path}" if python_path else str(project_root)
    env["PRODUCER_RATE"] = str(rate)

    process = subprocess.Popen(
        [sys.executable, str(runner_script)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        cwd=str(project_root),
    )

    # Write PID file so 'clickstream status' can detect the running producer
    PRODUCER_PID_FILE.write_text(str(process.pid))

    return process


def _stop_producer_process(process: subprocess.Popen) -> None:
    """Stop a producer subprocess gracefully.

    Sends SIGTERM and waits up to 10 seconds for graceful shutdown.
    Falls back to SIGKILL if the process doesn't exit.

    Args:
        process: The Popen handle for the producer
    """
    import signal as sig

    try:
        process.send_signal(sig.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    except ProcessLookupError:
        pass  # Already exited

    # Clean up PID file
    PRODUCER_PID_FILE.unlink(missing_ok=True)


def _compute_lag_trend(samples: list[tuple[float, int]]) -> str:
    """
    Compute lag trend from collected samples using linear regression.

    Uses the same algorithm as PipelineMetrics.get_lag_trend() but operates
    on an in-memory list of (timestamp, total_lag) tuples instead of Valkey.

    Args:
        samples: List of (unix_timestamp, total_lag) tuples

    Returns:
        One of: "growing", "stable", "shrinking", or "unknown"
    """
    if len(samples) < 2:
        return "unknown"

    n = len(samples)
    sum_x = sum(t for t, _ in samples)
    sum_y = sum(lag for _, lag in samples)
    sum_xy = sum(t * lag for t, lag in samples)
    sum_x2 = sum(t * t for t, _ in samples)

    denominator = n * sum_x2 - sum_x * sum_x
    if denominator == 0:
        return "unknown"

    slope = (n * sum_xy - sum_x * sum_y) / denominator

    mean_lag = sum_y / n
    if mean_lag == 0:
        return "stable" if abs(slope) < 1 else ("growing" if slope > 0 else "shrinking")

    relative_slope = abs(slope) / mean_lag
    if relative_slope < 0.01:
        return "stable"
    elif slope > 0:
        return "growing"
    else:
        return "shrinking"


# ==============================================================================
# Ramp Command
# ==============================================================================


def benchmark_ramp(
    start_rate: Annotated[
        int, typer.Option("--start-rate", help="Starting production rate (events/sec)")
    ],
    end_rate: Annotated[
        int, typer.Option("--end-rate", help="Maximum production rate to test (events/sec)")
    ],
    step: Annotated[int, typer.Option("--step", help="Rate increase per step (events/sec)")],
    step_duration: Annotated[
        int, typer.Option("--step-duration", help="Seconds to hold each rate level")
    ] = 60,
    impl: Annotated[
        Optional[str],
        typer.Option(
            "--consumer-impl",
            help="Override consumer implementation (confluent, kafka_python, quix, mage, bytewax)",
        ),
    ] = None,
    lag_threshold: Annotated[
        int,
        typer.Option("--lag-threshold", help="Stop early if total lag exceeds this value"),
    ] = 100_000,
    output: Annotated[Path, typer.Option("--output", "-o", help="Output CSV file")] = Path(
        "benchmark_ramp_results.csv"
    ),
    confirm: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
    quiet: Annotated[
        bool, typer.Option("--quiet", "-q", help="Suppress output for programmatic use")
    ] = False,
) -> None:
    """Run a ramp-up load test to find the consumer saturation point.

    Starts consumers, then gradually increases the production rate from
    --start-rate to --end-rate in increments of --step. At each rate level,
    holds for --step-duration seconds while sampling consumer lag and
    throughput every 5 seconds.

    The test stops early if consumer lag exceeds --lag-threshold, indicating
    the saturation point has been reached.

    Examples:
        clickstream benchmark ramp --start-rate 1000 --end-rate 10000 --step 1000 -y
        clickstream benchmark ramp --start-rate 500 --end-rate 5000 --step 500 --step-duration 30 -y
        clickstream benchmark ramp --start-rate 1000 --end-rate 20000 --step 2000 --consumer-impl confluent -y
    """
    from clickstream.infrastructure.kafka import get_consumer_lag
    from clickstream.utils.db import reset_schema
    from clickstream.utils.session_state import get_valkey_client

    # Helper for conditional printing
    def _print(msg: str = "") -> None:
        if not quiet:
            print(msg)

    # ── Validate options ─────────────────────────────────────────────────
    if start_rate <= 0:
        print(f"{C.BRIGHT_RED}{I.CROSS} --start-rate must be positive{C.RESET}")
        raise typer.Exit(1)

    if end_rate <= start_rate:
        print(f"{C.BRIGHT_RED}{I.CROSS} --end-rate must be greater than --start-rate{C.RESET}")
        raise typer.Exit(1)

    if step <= 0:
        print(f"{C.BRIGHT_RED}{I.CROSS} --step must be positive{C.RESET}")
        raise typer.Exit(1)

    if step_duration < 10:
        print(f"{C.BRIGHT_RED}{I.CROSS} --step-duration must be at least 10 seconds{C.RESET}")
        raise typer.Exit(1)

    # ── Validate --impl option ───────────────────────────────────────────
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
                f"{C.BRIGHT_RED}{I.CROSS} Invalid --consumer-impl value: {impl}. "
                f"Valid options: {', '.join(valid_impls)}{C.RESET}"
            )
            raise typer.Exit(1)
        from importlib.metadata import PackageNotFoundError, version as pkg_version

        package_name = impl_packages[impl]
        try:
            pkg_version(package_name)
        except PackageNotFoundError:
            print(
                f"{C.BRIGHT_RED}{I.CROSS} Package '{package_name}' is not installed. "
                f"Install it to use --consumer-impl {impl}{C.RESET}"
            )
            raise typer.Exit(1)

    # ── Compute steps ────────────────────────────────────────────────────
    rates = list(range(start_rate, end_rate + 1, step))
    if not rates:
        rates = [start_rate]
    total_steps = len(rates)
    samples_per_step = step_duration // RAMP_SAMPLE_INTERVAL

    # --quiet implies --yes
    if quiet:
        confirm = True

    settings = get_settings()
    partitions = settings.kafka.events_topic_partitions

    # Determine effective consumer impl
    effective_impl = impl if impl is not None else settings.consumer.impl

    # Get consumer info (temporarily override env if needed)
    original_impl = os.environ.get("CONSUMER_IMPL")
    if impl is not None:
        os.environ["CONSUMER_IMPL"] = impl
        from clickstream.utils.config import get_settings as _get_settings

        _get_settings.cache_clear()

    try:
        consumer = get_consumer("postgresql")
        num_instances = consumer.num_instances
    finally:
        if impl is not None:
            if original_impl is not None:
                os.environ["CONSUMER_IMPL"] = original_impl
            else:
                os.environ.pop("CONSUMER_IMPL", None)
            from clickstream.utils.config import get_settings as _get_settings

            _get_settings.cache_clear()

    # ── Configuration display ────────────────────────────────────────────
    _print()
    _print(f"  {C.BOLD}Ramp Test Configuration{C.RESET}")
    if impl is not None and impl != settings.consumer.impl:
        _print(
            f"  \u2022 Consumer Impl:        {C.WHITE}{effective_impl}{C.RESET} "
            f"{C.BRIGHT_YELLOW}(override){C.RESET}"
        )
    else:
        _print(f"  \u2022 Consumer Impl:        {C.WHITE}{effective_impl}{C.RESET}")
    _print(f"  \u2022 Partitions:           {C.WHITE}{partitions}{C.RESET}")
    _print(f"  \u2022 Consumers:            {C.WHITE}{consumer.parallelism_description}{C.RESET}")
    _print(
        f"  \u2022 Rate range:           {C.WHITE}{start_rate:,} \u2192 {end_rate:,} events/sec{C.RESET}"
    )
    _print(f"  \u2022 Step size:            {C.WHITE}{step:,} events/sec{C.RESET}")
    _print(f"  \u2022 Step duration:        {C.WHITE}{step_duration}s{C.RESET}")
    _print(f"  \u2022 Total steps:          {C.WHITE}{total_steps}{C.RESET}")
    _print(f"  \u2022 Lag threshold:        {C.WHITE}{lag_threshold:,}{C.RESET}")
    _print(f"  \u2022 Output file:          {C.WHITE}{output}{C.RESET}")
    est_duration = total_steps * (step_duration + RAMP_SAMPLE_INTERVAL)
    _print(f"  \u2022 Est. duration:        {C.WHITE}{_format_duration(est_duration)}{C.RESET}")
    _print()

    # ── Confirmation prompt ──────────────────────────────────────────────
    if not confirm:
        print(
            f"{C.BRIGHT_YELLOW}{I.WARN} This will reset all data before starting the ramp test.{C.RESET}"
        )
        typer.confirm("Continue?", abort=True)
        _print()

    # ── Pre-flight checks ────────────────────────────────────────────────
    _print(f"  {C.BOLD}Pre-flight checks{C.RESET}")

    running_consumers = count_running_consumers()
    if running_consumers > 0:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Consumer is running ({running_consumers} instances) - "
            f"stop it first with '{C.WHITE}clickstream consumer stop{C.BRIGHT_RED}'{C.RESET}"
        )
        raise typer.Exit(1)
    _print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Consumer is stopped")

    if is_process_running(PRODUCER_PID_FILE):
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Producer is running - "
            f"stop it first with '{C.WHITE}clickstream producer stop{C.BRIGHT_RED}'{C.RESET}"
        )
        raise typer.Exit(1)
    _print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Producer is stopped")

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

    # ── Reset data ───────────────────────────────────────────────────────
    _print()
    _print(f"  {C.BOLD}Preparing environment{C.RESET}")

    topic = settings.kafka.events_topic
    purge_kafka_topic(topic)
    reset_schema()
    client = get_valkey_client()
    client.flushall()
    consumer_group = settings.postgresql_consumer.group_id
    reset_consumer_group(consumer_group)
    _print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Data reset complete")

    # ── Start consumers ──────────────────────────────────────────────────
    project_root = get_project_root()
    runner_script = project_root / "clickstream" / "consumer_runner.py"

    for i in range(num_instances):
        start_consumer_instance(
            runner_script, i, project_root, benchmark_mode=False, impl_override=impl
        )
        if i < num_instances - 1:
            time.sleep(1)

    time.sleep(2)

    running = get_all_consumer_pids()
    if len(running) != num_instances:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Only {len(running)}/{num_instances} consumers started{C.RESET}"
        )
        stop_all_consumers()
        raise typer.Exit(1)

    _print(
        f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Consumers started "
        f"({consumer.parallelism_description})"
    )

    # Wait for consumers to connect and begin polling
    _print(f"  {C.DIM}Waiting for consumers to initialize...{C.RESET}")
    time.sleep(5)

    # ── Ramp loop ────────────────────────────────────────────────────────
    _print()
    _print(f"  {C.BOLD}Starting ramp test{C.RESET}")

    csv_rows: list[dict[str, str]] = []
    threshold_exceeded = False

    # Create Kafka clients for direct lag queries (reused across all samples)
    from kafka import KafkaAdminClient as _KafkaAdmin
    from kafka import KafkaConsumer as _KafkaConsumer

    from clickstream.infrastructure.kafka import build_kafka_config

    _kafka_cfg = build_kafka_config(request_timeout_ms=10000)
    lag_admin = _KafkaAdmin(**_kafka_cfg)
    lag_consumer = _KafkaConsumer(**_kafka_cfg)

    # Accumulate (timestamp, total_lag) tuples for inline trend calculation
    lag_samples: list[tuple[float, int]] = []

    try:
        with PgCounter() as pg:
            # Reconnect after schema reset
            pg.close()
            pg._connect()

            last_pg_count = pg.count(retries=1)
            if last_pg_count == -1:
                last_pg_count = 0

            for step_num, current_rate in enumerate(rates, 1):
                _print()
                _print(
                    f"  {C.BOLD}Step {step_num}/{total_steps} "
                    f"@ {current_rate:,} events/sec{C.RESET}"
                )

                # Start producer at current rate
                producer_proc = _run_producer_background(float(current_rate))
                # Brief pause for producer to connect to Kafka
                time.sleep(2)

                # Verify producer started
                if producer_proc.poll() is not None:
                    _print(
                        f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Producer failed to start "
                        f"at {current_rate:,}/sec"
                    )
                    PRODUCER_PID_FILE.unlink(missing_ok=True)
                    continue

                # Sample lag and throughput for this step
                # First iteration (sample_idx=0) is a warmup: establishes PG
                # baseline after producer startup but doesn't record a CSV row.
                for sample_idx in range(samples_per_step + 1):
                    time.sleep(RAMP_SAMPLE_INTERVAL)

                    # Check if producer is still running
                    if producer_proc.poll() is not None:
                        _print(f"  {C.BRIGHT_YELLOW}{I.WARN}{C.RESET} Producer exited during step")
                        break

                    # Query Kafka directly for real-time lag
                    partition_lags = get_consumer_lag(
                        consumer_group,
                        topic,
                        admin_client=lag_admin,
                        consumer=lag_consumer,
                    )
                    total_lag = sum(partition_lags.values())

                    # Accumulate for inline trend calculation (~2 min window)
                    lag_samples.append((time.time(), total_lag))
                    lag_trend = _compute_lag_trend(lag_samples[-24:])

                    # Calculate consumer throughput
                    current_pg_count = pg.count(retries=1)
                    if current_pg_count == -1:
                        current_pg_count = last_pg_count
                    consumer_throughput = max(
                        0, (current_pg_count - last_pg_count) // RAMP_SAMPLE_INTERVAL
                    )
                    last_pg_count = current_pg_count

                    # Warmup sample: just establish PG baseline, don't record
                    if sample_idx == 0:
                        continue

                    # Build CSV row
                    timestamp = datetime.now().isoformat(timespec="seconds")
                    row: dict[str, str] = {
                        "timestamp": timestamp,
                        "step": str(step_num),
                        "rate_target": str(current_rate),
                        "consumer_throughput": str(consumer_throughput),
                        "total_lag": str(total_lag),
                        "lag_trend": lag_trend,
                    }
                    # Add per-partition lag columns
                    for p_idx, p_lag in sorted(partition_lags.items()):
                        row[f"p{p_idx}_lag"] = str(p_lag)
                    csv_rows.append(row)

                    # Progress display
                    remaining = step_duration - (sample_idx * RAMP_SAMPLE_INTERVAL)
                    trend_symbol = {
                        "growing": f"{C.BRIGHT_RED}\u2191{C.RESET}",
                        "stable": f"{C.BRIGHT_GREEN}\u2192{C.RESET}",
                        "shrinking": f"{C.BRIGHT_CYAN}\u2193{C.RESET}",
                        "unknown": f"{C.DIM}?{C.RESET}",
                    }.get(lag_trend, "?")

                    if not quiet:
                        print(
                            f"\r  \u2022 {_format_abbreviated(consumer_throughput)}/s consumed "
                            f"| Lag: {total_lag:,} ({trend_symbol}) "
                            f"| {remaining}s left   ",
                            end="",
                            flush=True,
                        )

                if not quiet:
                    print()  # Newline after progress

                # Stop producer for this step
                _stop_producer_process(producer_proc)

                # Check lag threshold
                if total_lag > lag_threshold:
                    _print(
                        f"  {C.BRIGHT_YELLOW}{I.WARN} Lag threshold exceeded "
                        f"({total_lag:,} > {lag_threshold:,}), stopping ramp{C.RESET}"
                    )
                    threshold_exceeded = True
                    break

    except KeyboardInterrupt:
        _print(f"\n  {C.BRIGHT_RED}{I.CROSS} Ramp test interrupted{C.RESET}")
        # Clean up producer if running
        try:
            _stop_producer_process(producer_proc)  # type: ignore[possibly-undefined]
        except Exception:
            PRODUCER_PID_FILE.unlink(missing_ok=True)

    # ── Stop consumers ───────────────────────────────────────────────────
    stop_all_consumers()
    _print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Consumers stopped")

    # Close Kafka lag query clients
    try:
        lag_admin.close()
    except Exception:
        pass
    try:
        lag_consumer.close()
    except Exception:
        pass

    # ── Write CSV ────────────────────────────────────────────────────────
    if csv_rows:
        # Collect all partition columns across all rows
        partition_cols: list[str] = []
        for row in csv_rows:
            for key in row:
                if key.startswith("p") and key.endswith("_lag") and key not in partition_cols:
                    partition_cols.append(key)
        partition_cols.sort()

        base_cols = [
            "timestamp",
            "step",
            "rate_target",
            "consumer_throughput",
            "total_lag",
            "lag_trend",
        ]
        all_cols = base_cols + partition_cols

        write_header = not output.exists()
        with open(output, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            for row in csv_rows:
                writer.writerow(row)

        _print()
        _print(f"{C.BRIGHT_GREEN}{I.CHECK} Results appended to {output}{C.RESET}")

        # ── Summary ──────────────────────────────────────────────────────
        _print()
        _print(f"  {C.BOLD}Ramp Test Summary{C.RESET}")

        # Find saturation point from recorded data
        steps_seen: dict[int, list[str]] = {}
        for row in csv_rows:
            s = int(row["step"])
            if s not in steps_seen:
                steps_seen[s] = []
            steps_seen[s].append(row.get("lag_trend", "unknown"))

        saturation_rate: Optional[int] = None
        peak_sustainable: Optional[int] = None

        for s in sorted(steps_seen):
            trends = steps_seen[s]
            # Dominant trend
            trend_counts: dict[str, int] = {}
            for t in trends:
                trend_counts[t] = trend_counts.get(t, 0) + 1
            dominant = max(trend_counts, key=lambda k: trend_counts[k])
            rate_val = int(csv_rows[[int(r["step"]) for r in csv_rows].index(s)]["rate_target"])

            if dominant == "growing" and saturation_rate is None:
                saturation_rate = rate_val
            if dominant in ("stable", "shrinking"):
                peak_sustainable = rate_val

        if saturation_rate is not None:
            _print(
                f"  \u2022 Saturation point:  "
                f"{C.BRIGHT_YELLOW}{saturation_rate:,} events/sec{C.RESET}"
            )
        else:
            _print(f"  \u2022 Saturation point:  {C.BRIGHT_GREEN}Not reached{C.RESET}")

        if peak_sustainable is not None:
            _print(f"  \u2022 Peak sustainable:  {C.WHITE}{peak_sustainable:,} events/sec{C.RESET}")

        _print(f"  \u2022 Steps completed:  {C.WHITE}{len(steps_seen)}/{total_steps}{C.RESET}")
        _print(f"  \u2022 Total samples:    {C.WHITE}{len(csv_rows)}{C.RESET}")
        if threshold_exceeded:
            _print(f"  \u2022 Early stop:       {C.BRIGHT_YELLOW}Lag threshold exceeded{C.RESET}")
    else:
        _print()
        _print(f"{C.BRIGHT_RED}{I.CROSS} No data collected{C.RESET}")

    _print()
