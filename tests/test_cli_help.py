# ==============================================================================
# Tests for CLI Help Commands
# ==============================================================================
"""
Tests that all CLI help commands generate the expected output.

Verifies that every command and subcommand in the clickstream CLI:
- Exits with code 0 when invoked with --help
- Contains the expected description text
- Lists the expected subcommands or options

These tests use the real app from clickstream.app (not minimal Typer apps)
to ensure the full command tree is wired up correctly and that Typer can
introspect all command function signatures without errors.
"""

from typer.testing import CliRunner

from clickstream.app import app

runner = CliRunner()


# ==============================================================================
# Root App
# ==============================================================================


class TestRootHelp:
    """Tests for the root `clickstream --help` output."""

    def test_exit_code(self):
        """Root --help exits successfully."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Root --help shows the app description."""
        result = runner.invoke(app, ["--help"])
        assert "Clickstream data processing pipeline CLI" in result.output

    def test_lists_all_subcommands(self):
        """Root --help lists every top-level command."""
        result = runner.invoke(app, ["--help"])
        expected_commands = [
            "analytics",
            "benchmark",
            "config",
            "consumer",
            "data",
            "lag",
            "opensearch",
            "producer",
            "status",
        ]
        for cmd in expected_commands:
            assert cmd in result.output, f"Missing command: {cmd}"


# ==============================================================================
# Producer
# ==============================================================================


class TestProducerHelp:
    """Tests for `clickstream producer` help output."""

    def test_exit_code(self):
        """Producer --help exits successfully."""
        result = runner.invoke(app, ["producer", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Producer --help shows its description."""
        result = runner.invoke(app, ["producer", "--help"])
        assert "Producer pipeline operations" in result.output

    def test_lists_subcommands(self):
        """Producer --help lists all subcommands."""
        result = runner.invoke(app, ["producer", "--help"])
        for cmd in ["start", "stop", "logs", "list"]:
            assert cmd in result.output, f"Missing subcommand: {cmd}"


class TestProducerStartHelp:
    """Tests for `clickstream producer start --help` output."""

    def test_exit_code(self):
        """Producer start --help exits successfully."""
        result = runner.invoke(app, ["producer", "start", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Producer start --help shows its description."""
        result = runner.invoke(app, ["producer", "start", "--help"])
        assert "Start the producer pipeline" in result.output

    def test_lists_options(self):
        """Producer start --help lists all options."""
        result = runner.invoke(app, ["producer", "start", "--help"])
        for opt in ["--realtime", "--limit", "--rate", "--truncate-log"]:
            assert opt in result.output, f"Missing option: {opt}"


class TestProducerStopHelp:
    """Tests for `clickstream producer stop --help` output."""

    def test_exit_code(self):
        """Producer stop --help exits successfully."""
        result = runner.invoke(app, ["producer", "stop", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Producer stop --help shows its description."""
        result = runner.invoke(app, ["producer", "stop", "--help"])
        assert "Stop the running producer pipeline" in result.output


class TestProducerLogsHelp:
    """Tests for `clickstream producer logs --help` output."""

    def test_exit_code(self):
        """Producer logs --help exits successfully."""
        result = runner.invoke(app, ["producer", "logs", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Producer logs --help shows its description."""
        result = runner.invoke(app, ["producer", "logs", "--help"])
        assert "View producer log output" in result.output

    def test_lists_options(self):
        """Producer logs --help lists all options."""
        result = runner.invoke(app, ["producer", "logs", "--help"])
        for opt in ["--follow", "--lines"]:
            assert opt in result.output, f"Missing option: {opt}"


class TestProducerListHelp:
    """Tests for `clickstream producer list --help` output."""

    def test_exit_code(self):
        """Producer list --help exits successfully."""
        result = runner.invoke(app, ["producer", "list", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Producer list --help shows its description."""
        result = runner.invoke(app, ["producer", "list", "--help"])
        assert "List all available producer implementations" in result.output


# ==============================================================================
# Consumer
# ==============================================================================


class TestConsumerHelp:
    """Tests for `clickstream consumer` help output."""

    def test_exit_code(self):
        """Consumer --help exits successfully."""
        result = runner.invoke(app, ["consumer", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Consumer --help shows its description."""
        result = runner.invoke(app, ["consumer", "--help"])
        assert "Consumer pipeline operations" in result.output

    def test_lists_subcommands(self):
        """Consumer --help lists all subcommands."""
        result = runner.invoke(app, ["consumer", "--help"])
        for cmd in ["start", "stop", "restart", "logs", "list"]:
            assert cmd in result.output, f"Missing subcommand: {cmd}"


class TestConsumerStartHelp:
    """Tests for `clickstream consumer start --help` output."""

    def test_exit_code(self):
        """Consumer start --help exits successfully."""
        result = runner.invoke(app, ["consumer", "start", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Consumer start --help shows its description."""
        result = runner.invoke(app, ["consumer", "start", "--help"])
        assert "Start the consumer pipeline" in result.output


class TestConsumerStopHelp:
    """Tests for `clickstream consumer stop --help` output."""

    def test_exit_code(self):
        """Consumer stop --help exits successfully."""
        result = runner.invoke(app, ["consumer", "stop", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Consumer stop --help shows its description."""
        result = runner.invoke(app, ["consumer", "stop", "--help"])
        assert "Stop running consumer instances" in result.output


class TestConsumerRestartHelp:
    """Tests for `clickstream consumer restart --help` output."""

    def test_exit_code(self):
        """Consumer restart --help exits successfully."""
        result = runner.invoke(app, ["consumer", "restart", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Consumer restart --help shows its description."""
        result = runner.invoke(app, ["consumer", "restart", "--help"])
        assert "Restart consumer instances" in result.output


class TestConsumerLogsHelp:
    """Tests for `clickstream consumer logs --help` output."""

    def test_exit_code(self):
        """Consumer logs --help exits successfully."""
        result = runner.invoke(app, ["consumer", "logs", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Consumer logs --help shows its description."""
        result = runner.invoke(app, ["consumer", "logs", "--help"])
        assert "View consumer log output" in result.output

    def test_lists_options(self):
        """Consumer logs --help lists all options."""
        result = runner.invoke(app, ["consumer", "logs", "--help"])
        for opt in ["--follow", "--lines"]:
            assert opt in result.output, f"Missing option: {opt}"


class TestConsumerListHelp:
    """Tests for `clickstream consumer list --help` output."""

    def test_exit_code(self):
        """Consumer list --help exits successfully."""
        result = runner.invoke(app, ["consumer", "list", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Consumer list --help shows its description."""
        result = runner.invoke(app, ["consumer", "list", "--help"])
        assert "List all available consumer implementations" in result.output


# ==============================================================================
# Data
# ==============================================================================


class TestDataHelp:
    """Tests for `clickstream data` help output."""

    def test_exit_code(self):
        """Data --help exits successfully."""
        result = runner.invoke(app, ["data", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Data --help shows its description."""
        result = runner.invoke(app, ["data", "--help"])
        assert "Data management operations" in result.output

    def test_lists_subcommands(self):
        """Data --help lists all subcommands."""
        result = runner.invoke(app, ["data", "--help"])
        assert "reset" in result.output


class TestDataResetHelp:
    """Tests for `clickstream data reset --help` output."""

    def test_exit_code(self):
        """Data reset --help exits successfully."""
        result = runner.invoke(app, ["data", "reset", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Data reset --help shows its description."""
        result = runner.invoke(app, ["data", "reset", "--help"])
        assert "Reset" in result.output


# ==============================================================================
# OpenSearch
# ==============================================================================


class TestOpensearchHelp:
    """Tests for `clickstream opensearch` help output."""

    def test_exit_code(self):
        """OpenSearch --help exits successfully."""
        result = runner.invoke(app, ["opensearch", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """OpenSearch --help shows its description."""
        result = runner.invoke(app, ["opensearch", "--help"])
        assert "OpenSearch management operations" in result.output

    def test_lists_subcommands(self):
        """OpenSearch --help lists all subcommands."""
        result = runner.invoke(app, ["opensearch", "--help"])
        assert "init" in result.output


class TestOpensearchInitHelp:
    """Tests for `clickstream opensearch init --help` output."""

    def test_exit_code(self):
        """OpenSearch init --help exits successfully."""
        result = runner.invoke(app, ["opensearch", "init", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """OpenSearch init --help shows its description."""
        result = runner.invoke(app, ["opensearch", "init", "--help"])
        assert "OpenSearch" in result.output


# ==============================================================================
# Config
# ==============================================================================


class TestConfigHelp:
    """Tests for `clickstream config` help output."""

    def test_exit_code(self):
        """Config --help exits successfully."""
        result = runner.invoke(app, ["config", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Config --help shows its description."""
        result = runner.invoke(app, ["config", "--help"])
        assert "Configuration management" in result.output

    def test_lists_subcommands(self):
        """Config --help lists all subcommands."""
        result = runner.invoke(app, ["config", "--help"])
        for cmd in ["show", "generate"]:
            assert cmd in result.output, f"Missing subcommand: {cmd}"


class TestConfigShowHelp:
    """Tests for `clickstream config show --help` output."""

    def test_exit_code(self):
        """Config show --help exits successfully."""
        result = runner.invoke(app, ["config", "show", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Config show --help shows its description."""
        result = runner.invoke(app, ["config", "show", "--help"])
        assert "Show" in result.output


class TestConfigGenerateHelp:
    """Tests for `clickstream config generate --help` output."""

    def test_exit_code(self):
        """Config generate --help exits successfully."""
        result = runner.invoke(app, ["config", "generate", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Config generate --help shows its description."""
        result = runner.invoke(app, ["config", "generate", "--help"])
        assert "Generate" in result.output


# ==============================================================================
# Benchmark
# ==============================================================================


class TestBenchmarkHelp:
    """Tests for `clickstream benchmark` help output."""

    def test_exit_code(self):
        """Benchmark --help exits successfully."""
        result = runner.invoke(app, ["benchmark", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Benchmark --help shows its description."""
        result = runner.invoke(app, ["benchmark", "--help"])
        assert "Benchmark operations" in result.output

    def test_lists_subcommands(self):
        """Benchmark --help lists all subcommands."""
        result = runner.invoke(app, ["benchmark", "--help"])
        for cmd in ["run", "show", "ramp"]:
            assert cmd in result.output, f"Missing subcommand: {cmd}"


class TestBenchmarkRunHelp:
    """Tests for `clickstream benchmark run --help` output."""

    def test_exit_code(self):
        """Benchmark run --help exits successfully."""
        result = runner.invoke(app, ["benchmark", "run", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Benchmark run --help shows its description."""
        result = runner.invoke(app, ["benchmark", "run", "--help"])
        assert "Run a benchmark measuring consumer throughput" in result.output

    def test_lists_options(self):
        """Benchmark run --help lists all options."""
        result = runner.invoke(app, ["benchmark", "run", "--help"])
        for opt in ["--limit", "--output", "--yes", "--network", "--runs", "--increment", "--quiet", "--offset", "--consumer-impl"]:
            assert opt in result.output, f"Missing option: {opt}"


class TestBenchmarkShowHelp:
    """Tests for `clickstream benchmark show --help` output."""

    def test_exit_code(self):
        """Benchmark show --help exits successfully."""
        result = runner.invoke(app, ["benchmark", "show", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Benchmark show --help shows its description."""
        result = runner.invoke(app, ["benchmark", "show", "--help"])
        assert "Show benchmark results" in result.output

    def test_lists_options(self):
        """Benchmark show --help lists all options."""
        result = runner.invoke(app, ["benchmark", "show", "--help"])
        for opt in ["--file", "--ramp", "--run", "--all", "--environment", "--since", "--until", "--summary"]:
            assert opt in result.output, f"Missing option: {opt}"


class TestBenchmarkRampHelp:
    """Tests for `clickstream benchmark ramp --help` output."""

    def test_exit_code(self):
        """Benchmark ramp --help exits successfully."""
        result = runner.invoke(app, ["benchmark", "ramp", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Benchmark ramp --help shows its description."""
        result = runner.invoke(app, ["benchmark", "ramp", "--help"])
        assert "ramp-up load test" in result.output

    def test_lists_options(self):
        """Benchmark ramp --help lists all options."""
        result = runner.invoke(app, ["benchmark", "ramp", "--help"])
        for opt in ["--start-rate", "--end-rate", "--step", "--step-duration", "--consumer-impl", "--lag-threshold", "--output", "--yes", "--quiet"]:
            assert opt in result.output, f"Missing option: {opt}"


# ==============================================================================
# Lag
# ==============================================================================


class TestLagHelp:
    """Tests for `clickstream lag` help output."""

    def test_exit_code(self):
        """Lag --help exits successfully."""
        result = runner.invoke(app, ["lag", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Lag --help shows its description."""
        result = runner.invoke(app, ["lag", "--help"])
        assert "Consumer lag monitoring" in result.output

    def test_lists_subcommands(self):
        """Lag --help lists all subcommands."""
        result = runner.invoke(app, ["lag", "--help"])
        for cmd in ["show", "history", "report"]:
            assert cmd in result.output, f"Missing subcommand: {cmd}"


class TestLagShowHelp:
    """Tests for `clickstream lag show --help` output."""

    def test_exit_code(self):
        """Lag show --help exits successfully."""
        result = runner.invoke(app, ["lag", "show", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Lag show --help shows its description."""
        result = runner.invoke(app, ["lag", "show", "--help"])
        assert "Show" in result.output


class TestLagHistoryHelp:
    """Tests for `clickstream lag history --help` output."""

    def test_exit_code(self):
        """Lag history --help exits successfully."""
        result = runner.invoke(app, ["lag", "history", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Lag history --help shows its description."""
        result = runner.invoke(app, ["lag", "history", "--help"])
        assert "history" in result.output.lower()


class TestLagReportHelp:
    """Tests for `clickstream lag report --help` output."""

    def test_exit_code(self):
        """Lag report --help exits successfully."""
        result = runner.invoke(app, ["lag", "report", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Lag report --help shows its description."""
        result = runner.invoke(app, ["lag", "report", "--help"])
        assert "report" in result.output.lower()


# ==============================================================================
# Status & Analytics (top-level commands)
# ==============================================================================


class TestStatusHelp:
    """Tests for `clickstream status --help` output."""

    def test_exit_code(self):
        """Status --help exits successfully."""
        result = runner.invoke(app, ["status", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Status --help shows its description."""
        result = runner.invoke(app, ["status", "--help"])
        assert "status" in result.output.lower()


class TestAnalyticsHelp:
    """Tests for `clickstream analytics --help` output."""

    def test_exit_code(self):
        """Analytics --help exits successfully."""
        result = runner.invoke(app, ["analytics", "--help"])
        assert result.exit_code == 0

    def test_description(self):
        """Analytics --help shows its description."""
        result = runner.invoke(app, ["analytics", "--help"])
        assert "analytics" in result.output.lower()
