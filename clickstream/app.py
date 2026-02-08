# ==============================================================================
# Clickstream Pipeline CLI
# ==============================================================================
"""
Command-line interface for the clickstream data processing pipeline.

Usage:
    clickstream --help
    clickstream status
    clickstream config show
    clickstream config generate
    clickstream consumer start
    clickstream consumer stop
    clickstream producer start --limit 1000
    clickstream producer stop
    clickstream db init
    clickstream db reset -y
    clickstream data reset -y
"""

import logging
import warnings

# Suppress noisy warnings before any imports
warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.getLogger("traitlets").setLevel(logging.ERROR)

# ==============================================================================
# App Configuration
# ==============================================================================
# Set consistent terminal width for help output formatting
import os

import typer

if "COLUMNS" not in os.environ:
    os.environ["COLUMNS"] = "115"

app = typer.Typer(
    name="clickstream",
    help="Clickstream data processing pipeline CLI",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

producer_app = typer.Typer(
    help="Producer pipeline operations",
    no_args_is_help=True,
)
app.add_typer(producer_app, name="producer")

# Register producer commands from cli.producer module
from clickstream.cli.producer import producer_list, producer_logs, producer_start, producer_stop

producer_app.command("start")(producer_start)
producer_app.command("stop")(producer_stop)
producer_app.command("logs")(producer_logs)
producer_app.command("list")(producer_list)

consumer_app = typer.Typer(
    help="Consumer pipeline operations",
    no_args_is_help=True,
)
app.add_typer(consumer_app, name="consumer")

# Register consumer commands from cli.consumer module
from clickstream.cli.consumer import (
    consumer_list,
    consumer_logs,
    consumer_restart,
    consumer_start,
    consumer_stop,
)

consumer_app.command("start")(consumer_start)
consumer_app.command("stop")(consumer_stop)
consumer_app.command("restart")(consumer_restart)
consumer_app.command("logs")(consumer_logs)
consumer_app.command("list")(consumer_list)

data_app = typer.Typer(
    help="Data management operations",
    no_args_is_help=True,
)
app.add_typer(data_app, name="data")

# Register data commands from cli.data module
from clickstream.cli.data import data_reset

data_app.command("reset")(data_reset)

opensearch_app = typer.Typer(
    help="OpenSearch management operations",
    no_args_is_help=True,
)
app.add_typer(opensearch_app, name="opensearch")

# Register opensearch commands from cli.opensearch module
from clickstream.cli.opensearch import opensearch_init

opensearch_app.command("init")(opensearch_init)

config_app = typer.Typer(
    help="Configuration management",
    no_args_is_help=True,
)
app.add_typer(config_app, name="config")

# Register config commands from cli.config module
from clickstream.cli.config import config_generate, config_show

config_app.command("show")(config_show)
config_app.command("generate")(config_generate)


# Status command is imported from clickstream.cli.status
from clickstream.cli.status import show_status

app.command("status")(show_status)

# Analytics command is imported from clickstream.cli.analytics
from clickstream.cli.analytics import show_analytics

app.command("analytics")(show_analytics)

benchmark_app = typer.Typer(
    help="Benchmark operations",
    no_args_is_help=True,
)
app.add_typer(benchmark_app, name="benchmark")

from clickstream.cli.benchmark import benchmark_ramp, benchmark_run, benchmark_show

benchmark_app.command("run")(benchmark_run)
benchmark_app.command("show")(benchmark_show)
benchmark_app.command("ramp")(benchmark_ramp)

lag_app = typer.Typer(
    help="Consumer lag monitoring",
    no_args_is_help=True,
)
app.add_typer(lag_app, name="lag")

from clickstream.cli.lag import lag_history, lag_show

lag_app.command("show")(lag_show)
lag_app.command("history")(lag_history)


# ==============================================================================
# Entry Point
# ==============================================================================


def main() -> None:
    """CLI entry point."""
    app()


if __name__ == "__main__":
    main()
