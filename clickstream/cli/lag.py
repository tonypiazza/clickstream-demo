# ==============================================================================
# Consumer Lag Command
# ==============================================================================
"""
Consumer lag monitoring commands for the clickstream pipeline CLI.

Displays real-time consumer lag per partition and historical trends
using time-series data stored in Valkey.
"""

import json as _json
import re
from datetime import datetime
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from clickstream.cli.shared import C, I
from clickstream.infrastructure.metrics import get_lag_history, get_lag_trend
from clickstream.utils.config import get_settings


# ==============================================================================
# Helper Functions
# ==============================================================================

_WINDOW_PATTERN = re.compile(r"^(\d+)([smh])$", re.IGNORECASE)

_TREND_ICONS = {
    "growing": f"{C.BRIGHT_RED}↑ growing{C.RESET}",
    "stable": f"{C.BRIGHT_GREEN}→ stable{C.RESET}",
    "shrinking": f"{C.BRIGHT_CYAN}↓ shrinking{C.RESET}",
    "unknown": f"{C.DIM}? unknown{C.RESET}",
}


def _parse_window(window: str) -> int:
    """Parse a window string (e.g., '5m', '1h', '30s') to seconds.

    Args:
        window: Time window string with unit suffix (s=seconds, m=minutes, h=hours)

    Returns:
        Number of seconds

    Raises:
        typer.BadParameter: If format is invalid
    """
    match = _WINDOW_PATTERN.match(window)
    if not match:
        raise typer.BadParameter(
            f"Invalid window format: '{window}'. Use Ns, Nm, or Nh (e.g., 30s, 5m, 1h)"
        )
    value = int(match.group(1))
    unit = match.group(2).lower()
    multipliers = {"s": 1, "m": 60, "h": 3600}
    return value * multipliers[unit]


# ==============================================================================
# Commands
# ==============================================================================


def lag_show(
    json_output: Annotated[bool, typer.Option("--json", "-j", help="Output as JSON")] = False,
) -> None:
    """Show current consumer lag per partition with trend indicator.

    Displays the most recent lag sample for each partition along with a
    trend indicator (↑ growing, → stable, ↓ shrinking) based on the
    last 2 minutes of lag data.

    Examples:
        clickstream lag show
        clickstream lag show --json
    """
    settings = get_settings()
    group_id = settings.postgresql_consumer.group_id

    # Get most recent lag sample (last 60 seconds)
    history = get_lag_history(group_id, window_seconds=60)
    trend = get_lag_trend(group_id, window_seconds=120)

    if not history:
        if json_output:
            print(_json.dumps({"error": "No lag data available", "group_id": group_id}))
        else:
            print(
                f"\n  {C.BRIGHT_YELLOW}{I.WARN} No lag data available for "
                f"group '{group_id}'{C.RESET}"
            )
            print(f"  {C.DIM}Lag data is recorded while consumers are running.{C.RESET}")
            print()
        return

    # Use the most recent sample
    latest = history[-1]
    partitions = latest["partitions"]
    total_lag = latest["total"]
    sample_ts = datetime.fromtimestamp(latest["ts"])

    if json_output:
        print(
            _json.dumps(
                {
                    "group_id": group_id,
                    "timestamp": sample_ts.isoformat(),
                    "partitions": partitions,
                    "total_lag": total_lag,
                    "trend": trend,
                }
            )
        )
        return

    # Rich table output
    console = Console()
    table = Table(
        title=f"Consumer Lag — {group_id}",
        show_header=True,
        header_style="bold",
    )
    table.add_column("Partition", justify="right")
    table.add_column("Lag", justify="right")

    for part_idx in sorted(partitions.keys()):
        lag = partitions[part_idx]
        table.add_row(f"p{part_idx}", f"{lag:,}")

    print()
    console.print(table)
    print(f"  {C.BOLD}Total lag:{C.RESET}  {total_lag:,}")
    print(f"  {C.BOLD}Trend:{C.RESET}      {_TREND_ICONS.get(trend, trend)}")
    print(f"  {C.DIM}Sample at {sample_ts.strftime('%H:%M:%S')}{C.RESET}")
    print()


def lag_history(
    window: Annotated[
        str,
        typer.Option("--window", "-w", help="Time window (e.g., 30s, 5m, 1h)"),
    ] = "5m",
    json_output: Annotated[bool, typer.Option("--json", "-j", help="Output as JSON")] = False,
) -> None:
    """Show consumer lag history over a time window.

    Displays a time-series of lag samples showing how lag has changed
    over the specified window.

    Examples:
        clickstream lag history
        clickstream lag history --window 1h
        clickstream lag history -w 30s --json
    """
    settings = get_settings()
    group_id = settings.postgresql_consumer.group_id

    window_seconds = _parse_window(window)
    history = get_lag_history(group_id, window_seconds=window_seconds)
    trend = get_lag_trend(group_id, window_seconds=min(window_seconds, 120))

    if not history:
        if json_output:
            print(
                _json.dumps(
                    {
                        "error": "No lag data available",
                        "group_id": group_id,
                        "window": window,
                    }
                )
            )
        else:
            print(
                f"\n  {C.BRIGHT_YELLOW}{I.WARN} No lag data in the last "
                f"{window} for group '{group_id}'{C.RESET}"
            )
            print(f"  {C.DIM}Lag data is recorded while consumers are running.{C.RESET}")
            print()
        return

    if json_output:
        # Convert partition keys to strings for JSON compatibility
        json_history = []
        for sample in history:
            json_history.append(
                {
                    "timestamp": datetime.fromtimestamp(sample["ts"]).isoformat(),
                    "partitions": {str(k): v for k, v in sample["partitions"].items()},
                    "total": sample["total"],
                }
            )
        print(
            _json.dumps(
                {
                    "group_id": group_id,
                    "window": window,
                    "trend": trend,
                    "samples": json_history,
                }
            )
        )
        return

    # Discover all partition indices across all samples
    all_partitions: set[int] = set()
    for sample in history:
        all_partitions.update(sample["partitions"].keys())
    sorted_partitions = sorted(all_partitions)

    # Rich table output
    console = Console()
    table = Table(
        title=f"Lag History — {group_id} (last {window})",
        show_header=True,
        header_style="bold",
    )
    table.add_column("Time", justify="left")
    for p in sorted_partitions:
        table.add_column(f"p{p}", justify="right")
    table.add_column("Total", justify="right", style="bold")

    for sample in history:
        ts_str = datetime.fromtimestamp(sample["ts"]).strftime("%H:%M:%S")
        row = [ts_str]
        for p in sorted_partitions:
            lag = sample["partitions"].get(p)
            row.append(f"{lag:,}" if lag is not None else "—")
        row.append(f"{sample['total']:,}")
        table.add_row(*row)

    print()
    console.print(table)
    print(f"  {C.BOLD}Samples:{C.RESET}  {len(history)}")
    print(f"  {C.BOLD}Trend:{C.RESET}    {_TREND_ICONS.get(trend, trend)}")
    print()
