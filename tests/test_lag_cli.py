# ==============================================================================
# Tests for CLI Lag Commands
# ==============================================================================
"""
Unit tests for the `clickstream lag show` and `clickstream lag history` CLI commands.

Tests cover:
- Window string parsing (seconds, minutes, hours, invalid)
- `lag show` output (no data, with data, JSON mode)
- `lag history` output (no data, with data, JSON mode, custom window)

All tests mock the metrics functions (get_lag_history, get_lag_trend)
so no real Valkey connection is needed. CLI output is captured via
typer.testing.CliRunner.
"""

import json
import time
from unittest.mock import patch

import pytest
import typer
from typer.testing import CliRunner

from clickstream.cli.lag import _parse_window, lag_history, lag_report, lag_show

runner = CliRunner()


def _make_app():
    """Create a minimal Typer app with lag commands for testing."""
    app = typer.Typer()
    app.command("show")(lag_show)
    app.command("history")(lag_history)
    app.command("report")(lag_report)
    return app


# ==============================================================================
# _parse_window
# ==============================================================================


class TestParseWindow:
    """Tests for the window string parser."""

    def test_seconds(self):
        assert _parse_window("30s") == 30

    def test_minutes(self):
        assert _parse_window("5m") == 300

    def test_hours(self):
        assert _parse_window("1h") == 3600

    def test_uppercase(self):
        """Parser is case-insensitive."""
        assert _parse_window("2M") == 120
        assert _parse_window("1H") == 3600

    def test_invalid_unit_raises(self):
        with pytest.raises(typer.BadParameter, match="Invalid window format"):
            _parse_window("5x")

    def test_no_unit_raises(self):
        with pytest.raises(typer.BadParameter, match="Invalid window format"):
            _parse_window("100")

    def test_empty_raises(self):
        with pytest.raises(typer.BadParameter, match="Invalid window format"):
            _parse_window("")


# ==============================================================================
# lag show
# ==============================================================================

# Paths to mock in the lag CLI module (where they are imported)
_HISTORY_PATH = "clickstream.cli.lag.get_lag_history"
_TREND_PATH = "clickstream.cli.lag.get_lag_trend"
_SETTINGS_PATH = "clickstream.cli.lag.get_settings"


def _mock_settings():
    """Create a mock settings object with the consumer group ID."""
    from unittest.mock import MagicMock

    settings = MagicMock()
    settings.postgresql_consumer.group_id = "test-group"
    return settings


class TestLagShow:
    """Tests for the `lag show` command."""

    @patch(_SETTINGS_PATH)
    @patch(_TREND_PATH, return_value="unknown")
    @patch(_HISTORY_PATH, return_value=[])
    def test_no_data(self, mock_history, mock_trend, mock_settings):
        """Shows warning when no lag data is available."""
        mock_settings.return_value = _mock_settings()
        app = _make_app()
        result = runner.invoke(app, ["show"])
        assert result.exit_code == 0
        assert "No lag data" in result.output

    @patch(_SETTINGS_PATH)
    @patch(_TREND_PATH, return_value="stable")
    @patch(_HISTORY_PATH)
    def test_with_data(self, mock_history, mock_trend, mock_settings):
        """Shows table with partition lags and trend indicator."""
        mock_settings.return_value = _mock_settings()
        now = int(time.time())
        mock_history.return_value = [
            {"ts": now, "partitions": {0: 100, 1: 50, 2: 200}, "total": 350},
        ]
        app = _make_app()
        result = runner.invoke(app, ["show"])
        assert result.exit_code == 0
        assert "350" in result.output
        assert "stable" in result.output

    @patch(_SETTINGS_PATH)
    @patch(_TREND_PATH, return_value="growing")
    @patch(_HISTORY_PATH)
    def test_json_output(self, mock_history, mock_trend, mock_settings):
        """--json flag outputs valid JSON with correct structure."""
        mock_settings.return_value = _mock_settings()
        now = int(time.time())
        mock_history.return_value = [
            {"ts": now, "partitions": {0: 100, 1: 50}, "total": 150},
        ]
        app = _make_app()
        result = runner.invoke(app, ["show", "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert data["group_id"] == "test-group"
        assert data["total_lag"] == 150
        assert data["trend"] == "growing"
        assert "partitions" in data
        assert "timestamp" in data

    @patch(_SETTINGS_PATH)
    @patch(_TREND_PATH, return_value="unknown")
    @patch(_HISTORY_PATH, return_value=[])
    def test_no_data_json(self, mock_history, mock_trend, mock_settings):
        """--json with no data outputs JSON error object."""
        mock_settings.return_value = _mock_settings()
        app = _make_app()
        result = runner.invoke(app, ["show", "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert "error" in data
        assert data["group_id"] == "test-group"


# ==============================================================================
# lag history
# ==============================================================================


class TestLagHistory:
    """Tests for the `lag history` command."""

    @patch(_SETTINGS_PATH)
    @patch(_TREND_PATH, return_value="unknown")
    @patch(_HISTORY_PATH, return_value=[])
    def test_no_data(self, mock_history, mock_trend, mock_settings):
        """Shows warning when no lag history is available."""
        mock_settings.return_value = _mock_settings()
        app = _make_app()
        result = runner.invoke(app, ["history"])
        assert result.exit_code == 0
        assert "No lag data" in result.output

    @patch(_SETTINGS_PATH)
    @patch(_TREND_PATH, return_value="shrinking")
    @patch(_HISTORY_PATH)
    def test_with_data(self, mock_history, mock_trend, mock_settings):
        """Shows time-series table with partition columns."""
        mock_settings.return_value = _mock_settings()
        now = int(time.time())
        mock_history.return_value = [
            {"ts": now - 60, "partitions": {0: 500, 1: 300}, "total": 800},
            {"ts": now - 30, "partitions": {0: 300, 1: 200}, "total": 500},
            {"ts": now, "partitions": {0: 100, 1: 50}, "total": 150},
        ]
        app = _make_app()
        result = runner.invoke(app, ["history"])
        assert result.exit_code == 0
        # Should show sample count and trend
        assert "3" in result.output  # 3 samples
        assert "shrinking" in result.output

    @patch(_SETTINGS_PATH)
    @patch(_TREND_PATH, return_value="stable")
    @patch(_HISTORY_PATH)
    def test_json_output(self, mock_history, mock_trend, mock_settings):
        """--json flag outputs valid JSON with samples array."""
        mock_settings.return_value = _mock_settings()
        now = int(time.time())
        mock_history.return_value = [
            {"ts": now - 30, "partitions": {0: 200}, "total": 200},
            {"ts": now, "partitions": {0: 100}, "total": 100},
        ]
        app = _make_app()
        result = runner.invoke(app, ["history", "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert data["group_id"] == "test-group"
        assert data["trend"] == "stable"
        assert data["window"] == "5m"  # default
        assert len(data["samples"]) == 2
        assert "timestamp" in data["samples"][0]
        assert "total" in data["samples"][0]

    @patch(_SETTINGS_PATH)
    @patch(_TREND_PATH, return_value="stable")
    @patch(_HISTORY_PATH)
    def test_custom_window(self, mock_history, mock_trend, mock_settings):
        """--window 1h passes 3600 seconds to get_lag_history."""
        mock_settings.return_value = _mock_settings()
        mock_history.return_value = []
        app = _make_app()
        result = runner.invoke(app, ["history", "--window", "1h"])
        assert result.exit_code == 0

        # Verify get_lag_history was called with 3600 seconds
        mock_history.assert_called_once_with("test-group", window_seconds=3600)

    @patch(_SETTINGS_PATH)
    @patch(_TREND_PATH, return_value="unknown")
    @patch(_HISTORY_PATH, return_value=[])
    def test_no_data_json(self, mock_history, mock_trend, mock_settings):
        """--json with no data outputs JSON error object."""
        mock_settings.return_value = _mock_settings()
        app = _make_app()
        result = runner.invoke(app, ["history", "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert "error" in data
        assert data["group_id"] == "test-group"
        assert data["window"] == "5m"


# ==============================================================================
# lag report — Phase 4f
# ==============================================================================

_BACKPRESSURE_PATH = "clickstream.cli.lag.get_backpressure_report"


class TestLagReport:
    """Tests for the `lag report` command."""

    @patch(_SETTINGS_PATH)
    @patch(_BACKPRESSURE_PATH, return_value=None)
    @patch(_TREND_PATH, return_value="unknown")
    @patch(_HISTORY_PATH, return_value=[])
    def test_no_data(self, mock_history, mock_trend, mock_bp, mock_settings):
        """Shows warning when no data is available."""
        mock_settings.return_value = _mock_settings()
        app = _make_app()
        result = runner.invoke(app, ["report"])
        assert result.exit_code == 0
        assert "No data available" in result.output

    @patch(_SETTINGS_PATH)
    @patch(_BACKPRESSURE_PATH)
    @patch(_TREND_PATH, return_value="growing")
    @patch(_HISTORY_PATH)
    def test_with_data(self, mock_history, mock_trend, mock_bp, mock_settings):
        """Shows backpressure table with all indicators."""
        mock_settings.return_value = _mock_settings()
        now = int(time.time())
        mock_history.return_value = [
            {"ts": now, "partitions": {0: 40000, 1: 5230}, "total": 45230},
        ]
        mock_bp.return_value = {
            "fill_ratio": 0.98,
            "poll_proximity": 0.32,
            "idle_ms": 2.0,
            "rebalance_count": 0,
            "bottleneck_stage": "valkey (↑ 48%)",
            "instance_count": 2,
            "ts": now,
        }
        app = _make_app()
        result = runner.invoke(app, ["report"])
        assert result.exit_code == 0
        assert "45,230" in result.output
        assert "98%" in result.output
        assert "0.32" in result.output
        assert "2ms" in result.output
        assert "valkey" in result.output

    @patch(_SETTINGS_PATH)
    @patch(_BACKPRESSURE_PATH)
    @patch(_TREND_PATH, return_value="stable")
    @patch(_HISTORY_PATH)
    def test_json_output(self, mock_history, mock_trend, mock_bp, mock_settings):
        """--json flag outputs valid JSON with correct structure."""
        mock_settings.return_value = _mock_settings()
        now = int(time.time())
        mock_history.return_value = [
            {"ts": now, "partitions": {0: 100}, "total": 100},
        ]
        mock_bp.return_value = {
            "fill_ratio": 0.50,
            "poll_proximity": 0.10,
            "idle_ms": 80.0,
            "rebalance_count": 1,
            "bottleneck_stage": None,
            "instance_count": 1,
            "ts": now,
        }
        app = _make_app()
        result = runner.invoke(app, ["report", "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert data["group_id"] == "test-group"
        assert data["consumer_lag"] == 100
        assert data["lag_trend"] == "stable"
        assert data["fill_ratio"] == 0.50
        assert data["poll_proximity"] == 0.10
        assert data["idle_ms"] == 80.0
        assert data["rebalance_count"] == 1
        assert data["bottleneck_stage"] is None
        assert data["instance_count"] == 1

    @patch(_SETTINGS_PATH)
    @patch(_BACKPRESSURE_PATH, return_value=None)
    @patch(_TREND_PATH, return_value="unknown")
    @patch(_HISTORY_PATH, return_value=[])
    def test_no_data_json(self, mock_history, mock_trend, mock_bp, mock_settings):
        """--json with no data outputs JSON error object."""
        mock_settings.return_value = _mock_settings()
        app = _make_app()
        result = runner.invoke(app, ["report", "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert "error" in data
        assert data["group_id"] == "test-group"

    @patch(_SETTINGS_PATH)
    @patch(_BACKPRESSURE_PATH, return_value=None)
    @patch(_TREND_PATH, return_value="growing")
    @patch(_HISTORY_PATH)
    def test_lag_only_no_backpressure(self, mock_history, mock_trend, mock_bp, mock_settings):
        """Shows table with lag data but 'no data' for backpressure indicators."""
        mock_settings.return_value = _mock_settings()
        now = int(time.time())
        mock_history.return_value = [
            {"ts": now, "partitions": {0: 500}, "total": 500},
        ]
        app = _make_app()
        result = runner.invoke(app, ["report"])
        assert result.exit_code == 0
        # Lag should be shown
        assert "500" in result.output
        # Backpressure fields should show "no data"
        assert "no data" in result.output

    @patch(_SETTINGS_PATH)
    @patch(_BACKPRESSURE_PATH)
    @patch(_TREND_PATH, return_value="stable")
    @patch(_HISTORY_PATH)
    def test_saturated_fill_ratio_label(self, mock_history, mock_trend, mock_bp, mock_settings):
        """Fill ratio >= 95% shows 'saturated' status label."""
        mock_settings.return_value = _mock_settings()
        now = int(time.time())
        mock_history.return_value = [
            {"ts": now, "partitions": {0: 100}, "total": 100},
        ]
        mock_bp.return_value = {
            "fill_ratio": 0.99,
            "poll_proximity": 0.10,
            "idle_ms": 50.0,
            "rebalance_count": 0,
            "bottleneck_stage": None,
            "instance_count": 1,
            "ts": now,
        }
        app = _make_app()
        result = runner.invoke(app, ["report"])
        assert result.exit_code == 0
        assert "saturated" in result.output

    @patch(_SETTINGS_PATH)
    @patch(_BACKPRESSURE_PATH)
    @patch(_TREND_PATH, return_value="stable")
    @patch(_HISTORY_PATH)
    def test_healthy_proximity_label(self, mock_history, mock_trend, mock_bp, mock_settings):
        """Poll proximity <= 0.7 shows 'healthy' status label."""
        mock_settings.return_value = _mock_settings()
        now = int(time.time())
        mock_history.return_value = [
            {"ts": now, "partitions": {0: 100}, "total": 100},
        ]
        mock_bp.return_value = {
            "fill_ratio": 0.50,
            "poll_proximity": 0.25,
            "idle_ms": 50.0,
            "rebalance_count": 0,
            "bottleneck_stage": None,
            "instance_count": 1,
            "ts": now,
        }
        app = _make_app()
        result = runner.invoke(app, ["report"])
        assert result.exit_code == 0
        assert "healthy" in result.output
