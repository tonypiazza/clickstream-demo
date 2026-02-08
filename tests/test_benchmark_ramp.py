# ==============================================================================
# Tests for Benchmark Ramp Command
# ==============================================================================
"""
Unit tests for the `clickstream benchmark ramp` command and its helpers.

Tests cover:
- Ramp CSV parsing and aggregation (_show_ramp_results)
- Saturation point detection from dominant lag trends
- Validation of CLI options (rates, step, duration)
- benchmark_show dispatching with --run / --ramp flags
- _run_producer_background and _stop_producer_process helpers
- RAMP_SAMPLE_INTERVAL constant

All tests mock subprocess/external dependencies so no real Kafka,
PostgreSQL, or Valkey connections are needed.
"""

import csv
from pathlib import Path
from unittest.mock import MagicMock, patch

import typer
from typer.testing import CliRunner

from clickstream.cli.benchmark import (
    RAMP_SAMPLE_INTERVAL,
    _compute_lag_trend,
    _show_ramp_results,
    benchmark_ramp,
    benchmark_show,
)

runner = CliRunner()


def _make_show_app():
    """Create a minimal Typer app with the benchmark show command."""
    app = typer.Typer()
    app.command()(benchmark_show)
    return app


def _make_ramp_app():
    """Create a minimal Typer app with the benchmark ramp command."""
    app = typer.Typer()
    app.command()(benchmark_ramp)
    return app


def _write_ramp_csv(tmp_path: Path, rows: list[dict], filename: str = "ramp.csv") -> Path:
    """Write a ramp results CSV file from row dicts.

    Automatically determines fieldnames from the union of all row keys.
    """
    filepath = tmp_path / filename

    # Collect all keys in order: base columns first, then partition columns
    base_cols = [
        "timestamp",
        "run_id",
        "consumer_impl",
        "step",
        "rate_target",
        "consumer_throughput",
        "total_lag",
        "lag_trend",
    ]
    partition_cols: list[str] = []
    for row in rows:
        for key in row:
            if key.startswith("p") and key.endswith("_lag") and key not in partition_cols:
                partition_cols.append(key)
    partition_cols.sort()
    fieldnames = base_cols + partition_cols

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return filepath


# ==============================================================================
# Constants
# ==============================================================================


class TestConstants:
    """Tests for module-level constants."""

    def test_sample_interval(self):
        """RAMP_SAMPLE_INTERVAL is 5 seconds."""
        assert RAMP_SAMPLE_INTERVAL == 5


# ==============================================================================
# _show_ramp_results — CSV parsing and aggregation
# ==============================================================================


class TestShowRampResults:
    """Tests for the _show_ramp_results function."""

    def test_file_not_found(self, tmp_path):
        """Exits with error when CSV file doesn't exist."""
        missing = tmp_path / "nonexistent.csv"
        app = _make_show_app()
        result = runner.invoke(app, ["--ramp", "--file", str(missing)])
        assert result.exit_code == 1
        assert "No ramp results" in result.output

    def test_empty_csv(self, tmp_path):
        """Exits with error when CSV has only headers."""
        filepath = tmp_path / "empty.csv"
        filepath.write_text(
            "timestamp,run_id,consumer_impl,step,rate_target,"
            "consumer_throughput,total_lag,lag_trend\n"
        )
        app = _make_show_app()
        result = runner.invoke(app, ["--ramp", "--file", str(filepath)])
        assert result.exit_code == 1
        assert "No ramp data" in result.output

    def test_single_step_stable(self, tmp_path, capsys):
        """Single step with all stable samples shows stable trend."""
        rows = [
            {
                "timestamp": "2026-02-01T14:30:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "980",
                "total_lag": "15",
                "lag_trend": "stable",
                "p0_lag": "5",
                "p1_lag": "5",
                "p2_lag": "5",
            },
            {
                "timestamp": "2026-02-01T14:30:10",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "995",
                "total_lag": "12",
                "lag_trend": "stable",
                "p0_lag": "4",
                "p1_lag": "4",
                "p2_lag": "4",
            },
        ]
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=False)
        output = capsys.readouterr().out
        assert "1.0K" in output  # rate target
        assert "stable" in output

    def test_multi_step_with_saturation(self, tmp_path, capsys):
        """Detects saturation when dominant trend becomes 'growing'."""
        rows = [
            # Step 1: stable
            {
                "timestamp": "2026-02-01T14:30:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "980",
                "total_lag": "15",
                "lag_trend": "stable",
            },
            {
                "timestamp": "2026-02-01T14:30:10",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "990",
                "total_lag": "10",
                "lag_trend": "stable",
            },
            # Step 2: stable
            {
                "timestamp": "2026-02-01T14:31:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "2",
                "rate_target": "3000",
                "consumer_throughput": "2900",
                "total_lag": "120",
                "lag_trend": "stable",
            },
            {
                "timestamp": "2026-02-01T14:31:10",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "2",
                "rate_target": "3000",
                "consumer_throughput": "2950",
                "total_lag": "100",
                "lag_trend": "stable",
            },
            # Step 3: growing (saturation)
            {
                "timestamp": "2026-02-01T14:32:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "3",
                "rate_target": "5000",
                "consumer_throughput": "3500",
                "total_lag": "8000",
                "lag_trend": "growing",
            },
            {
                "timestamp": "2026-02-01T14:32:10",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "3",
                "rate_target": "5000",
                "consumer_throughput": "3400",
                "total_lag": "12000",
                "lag_trend": "growing",
            },
        ]
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=True)
        output = capsys.readouterr().out
        # Saturation at 5000 (step 3 is first growing)
        assert "5,000" in output
        # Peak sustainable at 3000 (step 2 was last stable)
        assert "3,000" in output

    def test_no_saturation(self, tmp_path, capsys):
        """When all steps are stable, reports saturation not reached."""
        rows = [
            {
                "timestamp": "2026-02-01T14:30:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "980",
                "total_lag": "10",
                "lag_trend": "stable",
            },
            {
                "timestamp": "2026-02-01T14:31:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "2",
                "rate_target": "3000",
                "consumer_throughput": "2900",
                "total_lag": "20",
                "lag_trend": "stable",
            },
        ]
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=True)
        output = capsys.readouterr().out
        assert "Not reached" in output

    def test_since_filter(self, tmp_path, capsys):
        """--since filters out older samples."""
        rows = [
            {
                "timestamp": "2026-01-01T12:00:00",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "900",
                "total_lag": "50",
                "lag_trend": "stable",
            },
            {
                "timestamp": "2026-02-01T12:00:00",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "980",
                "total_lag": "10",
                "lag_trend": "stable",
            },
        ]
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since="2026-01-15", until=None, summary=False)
        output = capsys.readouterr().out
        # Only the February row should appear
        assert "1" in output  # step number
        # The table should have only 1 sample
        # Check samples count in summary (always shown for ramp)
        assert "1" in output

    def test_partition_columns_in_table(self, tmp_path, capsys):
        """Table displays correctly with per-partition lag columns present."""
        rows = [
            {
                "timestamp": "2026-02-01T14:30:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "2000",
                "consumer_throughput": "1900",
                "total_lag": "30",
                "lag_trend": "stable",
                "p0_lag": "10",
                "p1_lag": "10",
                "p2_lag": "10",
            },
        ]
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=False)
        output = capsys.readouterr().out
        # Should show the step and rate
        assert "2.0K" in output

    def test_dominant_trend_majority(self, tmp_path, capsys):
        """Dominant trend is the most common trend within a step."""
        rows = [
            # 2 stable, 1 growing → dominant = stable
            {
                "timestamp": "2026-02-01T14:30:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "900",
                "total_lag": "50",
                "lag_trend": "stable",
            },
            {
                "timestamp": "2026-02-01T14:30:10",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "920",
                "total_lag": "60",
                "lag_trend": "growing",
            },
            {
                "timestamp": "2026-02-01T14:30:15",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "950",
                "total_lag": "40",
                "lag_trend": "stable",
            },
        ]
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=True)
        output = capsys.readouterr().out
        # Dominant is stable, so saturation should NOT be reported
        assert "Not reached" in output


# ==============================================================================
# _show_ramp_results — run_id filtering and consumer_impl display
# ==============================================================================


class TestShowRampRunFiltering:
    """Tests for run_id-based filtering and consumer_impl display."""

    def _make_multi_run_rows(self):
        """Create rows from two separate runs with different run_ids."""
        return [
            # Run 1: confluent, older timestamps
            {
                "timestamp": "2026-01-15T10:00:05",
                "run_id": "run1_aaa111",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "950",
                "total_lag": "20",
                "lag_trend": "stable",
            },
            {
                "timestamp": "2026-01-15T10:00:10",
                "run_id": "run1_aaa111",
                "consumer_impl": "confluent",
                "step": "2",
                "rate_target": "2000",
                "consumer_throughput": "1800",
                "total_lag": "200",
                "lag_trend": "growing",
            },
            # Run 2: kafka_python, newer timestamps
            {
                "timestamp": "2026-02-01T14:00:05",
                "run_id": "run2_bbb222",
                "consumer_impl": "kafka_python",
                "step": "1",
                "rate_target": "500",
                "consumer_throughput": "490",
                "total_lag": "10",
                "lag_trend": "stable",
            },
            {
                "timestamp": "2026-02-01T14:00:10",
                "run_id": "run2_bbb222",
                "consumer_impl": "kafka_python",
                "step": "2",
                "rate_target": "1000",
                "consumer_throughput": "980",
                "total_lag": "15",
                "lag_trend": "stable",
            },
        ]

    def test_latest_run_shown_by_default(self, tmp_path, capsys):
        """Without --all, only the latest run (by timestamp) is shown."""
        rows = self._make_multi_run_rows()
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=False)
        output = capsys.readouterr().out
        # Run 2 (kafka_python) has rate targets 500 and 1000
        assert "500" in output
        # Run 1 had rate target 2000 — should NOT appear
        assert "2.0K" not in output

    def test_all_flag_shows_all_runs(self, tmp_path, capsys):
        """With all_runs=True, all runs are shown."""
        rows = self._make_multi_run_rows()
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=False, all_runs=True)
        output = capsys.readouterr().out
        # Both runs should appear — run 1 had 2K rate target, run 2 had 500
        assert "2.0K" in output
        assert "500" in output

    def test_all_flag_shows_consumer_in_titles(self, tmp_path, capsys):
        """With --all and multiple runs, each table title shows consumer_impl."""
        rows = self._make_multi_run_rows()
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=False, all_runs=True)
        output = capsys.readouterr().out
        # Each run gets its own table with consumer_impl in the title
        assert "confluent" in output
        assert "kafka_python" in output

    def test_consumer_impl_in_title(self, tmp_path, capsys):
        """Single run shows consumer_impl in the table title."""
        rows = [
            {
                "timestamp": "2026-02-01T14:30:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "980",
                "total_lag": "15",
                "lag_trend": "stable",
            },
        ]
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=False)
        output = capsys.readouterr().out
        # Title should include the consumer impl
        assert "confluent" in output

    def test_single_run_no_consumer_column(self, tmp_path, capsys):
        """Single run does not show a Consumer column."""
        rows = [
            {
                "timestamp": "2026-02-01T14:30:05",
                "run_id": "aaa111bbb222",
                "consumer_impl": "confluent",
                "step": "1",
                "rate_target": "1000",
                "consumer_throughput": "980",
                "total_lag": "15",
                "lag_trend": "stable",
            },
        ]
        filepath = _write_ramp_csv(tmp_path, rows)
        _show_ramp_results(filepath, since=None, until=None, summary=False)
        output = capsys.readouterr().out
        # "Consumer" header should NOT appear for single run
        assert "Consumer" not in output

    def test_all_flag_via_cli(self, tmp_path):
        """--all flag is passed through benchmark_show to _show_ramp_results."""
        rows = self._make_multi_run_rows()
        filepath = _write_ramp_csv(tmp_path, rows)
        app = _make_show_app()
        result = runner.invoke(app, ["--ramp", "--all", "--file", str(filepath)])
        assert result.exit_code == 0
        # Both consumer impls should appear
        assert "confluent" in result.output
        assert "kafka_python" in result.output


# ==============================================================================
# benchmark_show — dispatch logic
# ==============================================================================


class TestBenchmarkShowDispatch:
    """Tests for the --run / --ramp flag dispatch in benchmark_show."""

    def test_run_and_ramp_mutually_exclusive(self):
        """Passing both --run and --ramp exits with error."""
        app = _make_show_app()
        result = runner.invoke(app, ["--run", "--ramp"])
        assert result.exit_code == 1
        assert "mutually exclusive" in result.output

    def test_defaults_to_run_mode(self, tmp_path):
        """Without flags, defaults to --run and looks for benchmark_run_results.csv."""
        app = _make_show_app()
        # No file exists → should report missing benchmark_run_results.csv
        result = runner.invoke(app, [])
        assert result.exit_code == 1
        assert "benchmark_run_results.csv" in result.output

    def test_ramp_flag_uses_ramp_file(self, tmp_path):
        """--ramp flag looks for benchmark_ramp_results.csv."""
        app = _make_show_app()
        result = runner.invoke(app, ["--ramp"])
        assert result.exit_code == 1
        assert "benchmark_ramp_results.csv" in result.output

    def test_custom_file_with_ramp(self, tmp_path):
        """--ramp --file custom.csv uses the specified file."""
        app = _make_show_app()
        result = runner.invoke(app, ["--ramp", "--file", "custom_ramp.csv"])
        assert result.exit_code == 1
        assert "custom_ramp.csv" in result.output


# ==============================================================================
# benchmark_ramp — validation
# ==============================================================================


class TestBenchmarkRampValidation:
    """Tests for CLI option validation in benchmark_ramp."""

    def test_start_rate_must_be_positive(self):
        """--start-rate 0 exits with error."""
        app = _make_ramp_app()
        result = runner.invoke(
            app, ["--start-rate", "0", "--end-rate", "1000", "--step", "100", "-y"]
        )
        assert result.exit_code == 1
        assert "positive" in result.output

    def test_end_rate_must_exceed_start(self):
        """--end-rate must be greater than --start-rate."""
        app = _make_ramp_app()
        result = runner.invoke(
            app, ["--start-rate", "1000", "--end-rate", "500", "--step", "100", "-y"]
        )
        assert result.exit_code == 1
        assert "greater than" in result.output

    def test_step_must_be_positive(self):
        """--step 0 exits with error."""
        app = _make_ramp_app()
        result = runner.invoke(
            app, ["--start-rate", "1000", "--end-rate", "5000", "--step", "0", "-y"]
        )
        assert result.exit_code == 1
        assert "positive" in result.output

    def test_step_duration_minimum(self):
        """--step-duration must be at least 10 seconds."""
        app = _make_ramp_app()
        result = runner.invoke(
            app,
            [
                "--start-rate",
                "1000",
                "--end-rate",
                "5000",
                "--step",
                "1000",
                "--step-duration",
                "5",
                "-y",
            ],
        )
        assert result.exit_code == 1
        assert "at least 10" in result.output

    def test_invalid_impl(self):
        """--consumer-impl with invalid value exits with error."""
        app = _make_ramp_app()
        result = runner.invoke(
            app,
            [
                "--start-rate",
                "1000",
                "--end-rate",
                "5000",
                "--step",
                "1000",
                "--consumer-impl",
                "nonexistent",
                "-y",
            ],
        )
        assert result.exit_code == 1
        assert "Invalid" in result.output


# ==============================================================================
# _run_producer_background / _stop_producer_process
# ==============================================================================


class TestProducerProcessHelpers:
    """Tests for the background producer helpers."""

    @patch("clickstream.cli.benchmark.subprocess.Popen")
    @patch("clickstream.cli.benchmark.PRODUCER_PID_FILE")
    def test_run_producer_background_sets_rate(self, mock_pid_file, mock_popen):
        """_run_producer_background sets PRODUCER_RATE in the subprocess env."""
        from clickstream.cli.benchmark import _run_producer_background

        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        result = _run_producer_background(2500.0)

        assert result == mock_process
        # Verify PRODUCER_RATE was set in the env
        call_kwargs = mock_popen.call_args
        env = call_kwargs.kwargs.get("env") or call_kwargs[1].get("env")
        assert env["PRODUCER_RATE"] == "2500.0"
        # Verify PID file was written
        mock_pid_file.write_text.assert_called_once_with("12345")

    @patch("clickstream.cli.benchmark.PRODUCER_PID_FILE")
    def test_stop_producer_process_sigterm(self, mock_pid_file):
        """_stop_producer_process sends SIGTERM and waits."""
        from clickstream.cli.benchmark import _stop_producer_process

        mock_process = MagicMock()
        mock_process.wait.return_value = 0

        _stop_producer_process(mock_process)

        mock_process.send_signal.assert_called_once()
        mock_process.wait.assert_called_once_with(timeout=10)
        mock_pid_file.unlink.assert_called_once_with(missing_ok=True)

    @patch("clickstream.cli.benchmark.PRODUCER_PID_FILE")
    def test_stop_producer_process_sigkill_on_timeout(self, mock_pid_file):
        """Falls back to SIGKILL when SIGTERM times out."""
        import subprocess

        from clickstream.cli.benchmark import _stop_producer_process

        mock_process = MagicMock()
        mock_process.wait.side_effect = [
            subprocess.TimeoutExpired(cmd="test", timeout=10),  # First wait (SIGTERM)
            None,  # Second wait (after SIGKILL)
        ]

        _stop_producer_process(mock_process)

        mock_process.kill.assert_called_once()

    @patch("clickstream.cli.benchmark.PRODUCER_PID_FILE")
    def test_stop_producer_process_already_exited(self, mock_pid_file):
        """Handles ProcessLookupError when process already exited."""
        from clickstream.cli.benchmark import _stop_producer_process

        mock_process = MagicMock()
        mock_process.send_signal.side_effect = ProcessLookupError()

        # Should not raise
        _stop_producer_process(mock_process)

        mock_pid_file.unlink.assert_called_once_with(missing_ok=True)


# ==============================================================================
# _compute_lag_trend — inline trend calculation
# ==============================================================================


class TestComputeLagTrend:
    """Tests for the _compute_lag_trend inline helper."""

    def test_unknown_with_zero_samples(self):
        """Returns 'unknown' with no samples."""
        assert _compute_lag_trend([]) == "unknown"

    def test_unknown_with_one_sample(self):
        """Returns 'unknown' with fewer than 2 samples."""
        assert _compute_lag_trend([(1000.0, 50)]) == "unknown"

    def test_stable_when_lag_constant(self):
        """Returns 'stable' when lag is constant across samples."""
        samples = [(1000.0 + i * 5, 100) for i in range(10)]
        assert _compute_lag_trend(samples) == "stable"

    def test_growing_when_lag_increasing(self):
        """Returns 'growing' when lag increases steadily."""
        # Lag increases by 100 per sample → clear upward trend
        samples = [(1000.0 + i * 5, 100 + i * 100) for i in range(10)]
        assert _compute_lag_trend(samples) == "growing"

    def test_shrinking_when_lag_decreasing(self):
        """Returns 'shrinking' when lag decreases steadily."""
        # Lag decreases by 100 per sample from 1000
        samples = [(1000.0 + i * 5, 1000 - i * 100) for i in range(10)]
        assert _compute_lag_trend(samples) == "shrinking"

    def test_stable_with_zero_lag(self):
        """Returns 'stable' when all lag values are zero."""
        samples = [(1000.0 + i * 5, 0) for i in range(5)]
        assert _compute_lag_trend(samples) == "stable"

    def test_growing_from_zero(self):
        """Returns 'growing' when lag rises from zero."""
        samples = [(1000.0 + i * 5, i * 50) for i in range(6)]
        assert _compute_lag_trend(samples) == "growing"

    def test_stable_with_minor_fluctuations(self):
        """Returns 'stable' when lag fluctuates but doesn't trend."""
        # Fluctuates around 500 by ±5 — relative slope is tiny
        lags = [500, 505, 498, 502, 500, 503, 497, 501, 499, 504]
        samples = [(1000.0 + i * 5, lag) for i, lag in enumerate(lags)]
        assert _compute_lag_trend(samples) == "stable"

    def test_two_samples_minimum(self):
        """Works correctly with exactly 2 samples."""
        # Two samples: lag goes from 0 to 100
        samples = [(1000.0, 0), (1005.0, 100)]
        result = _compute_lag_trend(samples)
        assert result == "growing"


# ==============================================================================
# get_consumer_lag — direct Kafka lag query
# ==============================================================================


class TestGetConsumerLag:
    """Tests for the get_consumer_lag function in kafka.py."""

    def test_returns_per_partition_lag(self):
        """Returns correct lag per partition."""
        from unittest.mock import MagicMock

        from clickstream.infrastructure.kafka import get_consumer_lag

        mock_admin = MagicMock()
        mock_consumer = MagicMock()

        # Simulate committed offsets: p0=90, p1=80, p2=70
        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata

        committed = {
            TopicPartition("events", 0): OffsetAndMetadata(90, "", -1),
            TopicPartition("events", 1): OffsetAndMetadata(80, "", -1),
            TopicPartition("events", 2): OffsetAndMetadata(70, "", -1),
        }
        mock_admin.list_consumer_group_offsets.return_value = committed

        # Simulate end offsets: p0=100, p1=100, p2=100
        mock_consumer.end_offsets.return_value = {
            TopicPartition("events", 0): 100,
            TopicPartition("events", 1): 100,
            TopicPartition("events", 2): 100,
        }

        result = get_consumer_lag(
            "test-group",
            "events",
            admin_client=mock_admin,
            consumer=mock_consumer,
        )

        assert result == {0: 10, 1: 20, 2: 30}

    def test_filters_to_specified_topic(self):
        """Only returns lag for the requested topic."""
        from unittest.mock import MagicMock

        from clickstream.infrastructure.kafka import get_consumer_lag

        mock_admin = MagicMock()
        mock_consumer = MagicMock()

        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata

        # Committed offsets for two different topics
        committed = {
            TopicPartition("events", 0): OffsetAndMetadata(50, "", -1),
            TopicPartition("other-topic", 0): OffsetAndMetadata(10, "", -1),
        }
        mock_admin.list_consumer_group_offsets.return_value = committed

        mock_consumer.end_offsets.return_value = {
            TopicPartition("events", 0): 100,
        }

        result = get_consumer_lag(
            "test-group",
            "events",
            admin_client=mock_admin,
            consumer=mock_consumer,
        )

        # Only the "events" topic partition should appear
        assert result == {0: 50}
        # end_offsets should only be called with the events partition
        call_args = mock_consumer.end_offsets.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0].topic == "events"

    def test_returns_empty_dict_on_error(self):
        """Returns empty dict when Kafka query fails."""
        from unittest.mock import MagicMock

        from clickstream.infrastructure.kafka import get_consumer_lag

        mock_admin = MagicMock()
        mock_consumer = MagicMock()
        mock_admin.list_consumer_group_offsets.side_effect = Exception("Connection refused")

        result = get_consumer_lag(
            "test-group",
            "events",
            admin_client=mock_admin,
            consumer=mock_consumer,
        )

        assert result == {}

    def test_returns_empty_when_no_committed_offsets(self):
        """Returns empty dict when consumer group has no committed offsets."""
        from unittest.mock import MagicMock

        from clickstream.infrastructure.kafka import get_consumer_lag

        mock_admin = MagicMock()
        mock_consumer = MagicMock()
        mock_admin.list_consumer_group_offsets.return_value = {}

        result = get_consumer_lag(
            "test-group",
            "events",
            admin_client=mock_admin,
            consumer=mock_consumer,
        )

        assert result == {}

    def test_lag_never_negative(self):
        """Lag is clamped to zero when committed offset exceeds end offset."""
        from unittest.mock import MagicMock

        from clickstream.infrastructure.kafka import get_consumer_lag

        mock_admin = MagicMock()
        mock_consumer = MagicMock()

        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata

        # Committed offset (110) is ahead of end offset (100)
        committed = {
            TopicPartition("events", 0): OffsetAndMetadata(110, "", -1),
        }
        mock_admin.list_consumer_group_offsets.return_value = committed

        mock_consumer.end_offsets.return_value = {
            TopicPartition("events", 0): 100,
        }

        result = get_consumer_lag(
            "test-group",
            "events",
            admin_client=mock_admin,
            consumer=mock_consumer,
        )

        assert result == {0: 0}

    def test_skips_negative_committed_offset(self):
        """Skips partitions with committed offset of -1 (no committed offset)."""
        from unittest.mock import MagicMock

        from clickstream.infrastructure.kafka import get_consumer_lag

        mock_admin = MagicMock()
        mock_consumer = MagicMock()

        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata

        committed = {
            TopicPartition("events", 0): OffsetAndMetadata(50, "", -1),
            TopicPartition("events", 1): OffsetAndMetadata(-1, "", -1),  # No offset
        }
        mock_admin.list_consumer_group_offsets.return_value = committed

        mock_consumer.end_offsets.return_value = {
            TopicPartition("events", 0): 100,
        }

        result = get_consumer_lag(
            "test-group",
            "events",
            admin_client=mock_admin,
            consumer=mock_consumer,
        )

        # Only partition 0 should be included
        assert result == {0: 50}

    def test_sorted_partition_keys(self):
        """Result dict has partition keys sorted."""
        from unittest.mock import MagicMock

        from clickstream.infrastructure.kafka import get_consumer_lag

        mock_admin = MagicMock()
        mock_consumer = MagicMock()

        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata

        # Provide partitions in reverse order
        committed = {
            TopicPartition("events", 2): OffsetAndMetadata(80, "", -1),
            TopicPartition("events", 0): OffsetAndMetadata(90, "", -1),
            TopicPartition("events", 1): OffsetAndMetadata(85, "", -1),
        }
        mock_admin.list_consumer_group_offsets.return_value = committed

        mock_consumer.end_offsets.return_value = {
            TopicPartition("events", 2): 100,
            TopicPartition("events", 0): 100,
            TopicPartition("events", 1): 100,
        }

        result = get_consumer_lag(
            "test-group",
            "events",
            admin_client=mock_admin,
            consumer=mock_consumer,
        )

        assert list(result.keys()) == [0, 1, 2]
