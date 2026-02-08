# ==============================================================================
# Tests for BatchMetrics — batch_processor.py
# ==============================================================================
"""
Tests for the BatchMetrics class, focusing on the record_loop_timing()
method and its integration with periodic summaries and final summary.
"""

import logging
import time
from unittest.mock import MagicMock

import pytest

from clickstream.consumers.batch_processor import BatchMetrics, _precision

# ==============================================================================
# Helpers
# ==============================================================================


def _make_batch_metrics(
    summary_interval: float = 30.0,
    on_summary=None,
    max_poll_interval_ms: int = 300_000,
) -> BatchMetrics:
    """Create a BatchMetrics with mock repos for unit testing."""
    event_repo = MagicMock()
    session_state = MagicMock()
    session_repo = MagicMock()

    # batch_update_sessions returns a list of session objects
    session_state.batch_update_sessions.return_value = [{"session_id": "s1"}]
    # to_db_record returns a dict for each session
    session_state.to_db_record.return_value = {"session_id": "s1", "event_count": 1}

    return BatchMetrics(
        event_repo=event_repo,
        session_state=session_state,
        session_repo=session_repo,
        summary_interval_seconds=summary_interval,
        on_summary=on_summary,
        max_poll_interval_ms=max_poll_interval_ms,
    )


# ==============================================================================
# _precision helper
# ==============================================================================


class TestPrecision:
    """Tests for the _precision helper function."""

    def test_large_values_zero_decimals(self):
        assert _precision(10.0) == 0
        assert _precision(85.3) == 0

    def test_medium_values_one_decimal(self):
        assert _precision(1.0) == 1
        assert _precision(3.2) == 1

    def test_small_values_two_decimals(self):
        assert _precision(0.5) == 2
        assert _precision(0.01) == 2


# ==============================================================================
# record_loop_timing — field accumulation
# ==============================================================================


class TestRecordLoopTiming:
    """Tests for the record_loop_timing method."""

    def test_single_recording(self):
        """A single call accumulates cumulative and period stats."""
        bm = _make_batch_metrics()
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        assert bm._cum_poll_ms == 5.0
        assert bm._cum_commit_ms == 3.0
        assert bm._cum_loop_count == 1
        assert bm._cum_fill_sum == pytest.approx(0.8)

        assert bm._period_poll_ms == 5.0
        assert bm._period_commit_ms == 3.0
        assert bm._period_loop_count == 1
        assert len(bm._period_fill_ratios) == 1
        assert bm._period_fill_ratios[0] == pytest.approx(0.8)

    def test_multiple_recordings_accumulate(self):
        """Multiple calls accumulate correctly."""
        bm = _make_batch_metrics()
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)
        bm.record_loop_timing(poll_ms=10.0, commit_ms=4.0, batch_size=1000, max_batch_size=1000)

        assert bm._cum_poll_ms == 15.0
        assert bm._cum_commit_ms == 7.0
        assert bm._cum_loop_count == 2
        assert bm._cum_fill_sum == pytest.approx(1.8)  # 0.8 + 1.0

        assert bm._period_poll_ms == 15.0
        assert bm._period_loop_count == 2
        assert len(bm._period_fill_ratios) == 2

    def test_fill_ratio_100_percent(self):
        """Full batch produces fill ratio of 1.0."""
        bm = _make_batch_metrics()
        bm.record_loop_timing(poll_ms=1.0, commit_ms=1.0, batch_size=10000, max_batch_size=10000)
        assert bm._period_fill_ratios[0] == pytest.approx(1.0)

    def test_fill_ratio_zero_max(self):
        """Zero max_batch_size produces fill ratio of 0."""
        bm = _make_batch_metrics()
        bm.record_loop_timing(poll_ms=1.0, commit_ms=1.0, batch_size=100, max_batch_size=0)
        assert bm._period_fill_ratios[0] == 0.0

    def test_no_loop_timing_recorded(self):
        """Loop timing fields start at zero."""
        bm = _make_batch_metrics()
        assert bm._cum_poll_ms == 0.0
        assert bm._cum_commit_ms == 0.0
        assert bm._cum_loop_count == 0
        assert bm._period_loop_count == 0
        assert bm._period_fill_ratios == []


# ==============================================================================
# _log_summary — loop timing in periodic summaries
# ==============================================================================


class TestLogSummaryLoopTiming:
    """Tests for loop timing appearing in periodic summaries."""

    def test_summary_includes_loop_timing(self, caplog):
        """When loop timing is recorded, summary includes poll/commit/fill."""
        bm = _make_batch_metrics(summary_interval=0.01)

        # Process a batch to trigger summary
        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        # Wait for summary interval
        time.sleep(0.02)

        with caplog.at_level(logging.INFO):
            bm.process_batch([{"event": "test2"}])

        # Find the loop timing log line
        loop_messages = [r.message for r in caplog.records if "avg_poll=" in r.message]
        assert len(loop_messages) >= 1
        msg = loop_messages[0]
        assert "avg_poll=" in msg
        assert "avg_commit=" in msg
        assert "fill=" in msg

    def test_summary_without_loop_timing(self, caplog):
        """When no loop timing is recorded, loop line is not logged."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.process_batch([{"event": "test"}])
        time.sleep(0.02)

        with caplog.at_level(logging.INFO):
            bm.process_batch([{"event": "test2"}])

        loop_messages = [r.message for r in caplog.records if "avg_poll=" in r.message]
        assert len(loop_messages) == 0

    def test_summary_resets_period_loop_counters(self):
        """After summary, period loop counters are reset."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        time.sleep(0.02)

        # Trigger summary via next batch
        bm.process_batch([{"event": "test2"}])

        # Period counters should be reset (only the batch from trigger remains)
        assert bm._period_poll_ms == 0.0
        assert bm._period_commit_ms == 0.0
        assert bm._period_loop_count == 0
        assert bm._period_fill_ratios == []

        # Cumulative counters should persist
        assert bm._cum_poll_ms == 5.0
        assert bm._cum_commit_ms == 3.0
        assert bm._cum_loop_count == 1

    def test_fill_percentage_in_summary(self, caplog):
        """Fill ratio appears as integer percentage in summary."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.process_batch([{"event": "test"}])
        # 95% fill
        bm.record_loop_timing(poll_ms=1.0, commit_ms=1.0, batch_size=9500, max_batch_size=10000)

        time.sleep(0.02)

        with caplog.at_level(logging.INFO):
            bm.process_batch([{"event": "test2"}])

        loop_messages = [r.message for r in caplog.records if "fill=" in r.message]
        assert any("fill=95%" in msg for msg in loop_messages)


# ==============================================================================
# log_final_summary — loop timing in final summary
# ==============================================================================


class TestLogFinalSummaryLoopTiming:
    """Tests for loop timing appearing in the final summary."""

    def test_final_summary_includes_loop_timing(self, caplog):
        """Final summary includes poll/commit/fill averages."""
        bm = _make_batch_metrics()

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)
        bm.record_loop_timing(poll_ms=10.0, commit_ms=4.0, batch_size=1000, max_batch_size=1000)

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        loop_messages = [r.message for r in caplog.records if "Final loop:" in r.message]
        assert len(loop_messages) == 1
        msg = loop_messages[0]
        assert "avg_poll=" in msg
        assert "avg_commit=" in msg
        assert "fill=" in msg

    def test_final_summary_without_loop_timing(self, caplog):
        """Final summary skips loop line when no loop timing was recorded."""
        bm = _make_batch_metrics()

        bm.process_batch([{"event": "test"}])

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        loop_messages = [r.message for r in caplog.records if "Final loop:" in r.message]
        assert len(loop_messages) == 0

    def test_no_batches_no_loop_line(self, caplog):
        """Final summary with no batches doesn't log loop timing."""
        bm = _make_batch_metrics()

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        messages = [r.message for r in caplog.records]
        assert any("no batches processed" in m for m in messages)
        assert not any("Final loop:" in m for m in messages)

    def test_final_summary_fill_percentage(self, caplog):
        """Final fill ratio is averaged correctly across all recordings."""
        bm = _make_batch_metrics()

        bm.process_batch([{"event": "test"}])
        # 50% + 100% → average 75%
        bm.record_loop_timing(poll_ms=1.0, commit_ms=1.0, batch_size=500, max_batch_size=1000)
        bm.record_loop_timing(poll_ms=1.0, commit_ms=1.0, batch_size=1000, max_batch_size=1000)

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        loop_messages = [r.message for r in caplog.records if "Final loop:" in r.message]
        assert any("fill=75%" in msg for msg in loop_messages)


# ==============================================================================
# process_batch — existing behavior preserved
# ==============================================================================


class TestProcessBatchUnchanged:
    """Verify that existing process_batch behavior is not broken."""

    def test_empty_batch_returns_empty(self):
        """Empty input returns empty list."""
        bm = _make_batch_metrics()
        assert bm.process_batch([]) == []

    def test_batch_updates_counters(self):
        """A batch increments cumulative and period counters."""
        bm = _make_batch_metrics()
        bm.process_batch([{"event": "test"}])

        assert bm._total_events == 1
        assert bm._total_batches == 1
        assert bm._period_events == 1
        assert bm._period_batches == 1

    def test_on_summary_callback_fires(self):
        """on_summary callback is called during periodic summary."""
        callback = MagicMock()
        bm = _make_batch_metrics(summary_interval=0.01, on_summary=callback)

        bm.process_batch([{"event": "test"}])
        time.sleep(0.02)
        bm.process_batch([{"event": "test2"}])

        callback.assert_called()


# ==============================================================================
# Max poll interval proximity — Phase 4b
# ==============================================================================


class TestProximityCalculation:
    """Tests for max poll interval proximity in record_loop_timing."""

    def test_proximity_computed_from_loop_time(self):
        """Proximity = (poll + batch + commit) / max_poll_interval."""
        bm = _make_batch_metrics(max_poll_interval_ms=1000)
        # Simulate process_batch setting _last_batch_total_ms
        bm._last_batch_total_ms = 200.0
        bm.record_loop_timing(poll_ms=100.0, commit_ms=50.0, batch_size=500, max_batch_size=1000)
        # loop_ms = 100 + 200 + 50 = 350; proximity = 350 / 1000 = 0.35
        assert bm._period_max_proximity == pytest.approx(0.35)
        assert bm._cum_max_proximity == pytest.approx(0.35)

    def test_proximity_tracks_max_in_period(self):
        """Period max proximity tracks the highest value seen."""
        bm = _make_batch_metrics(max_poll_interval_ms=1000)
        bm._last_batch_total_ms = 100.0
        # First: loop = 50 + 100 + 50 = 200 → 0.20
        bm.record_loop_timing(poll_ms=50.0, commit_ms=50.0, batch_size=500, max_batch_size=1000)
        assert bm._period_max_proximity == pytest.approx(0.20)

        # Second: loop = 100 + 100 + 200 = 400 → 0.40 (new max)
        bm.record_loop_timing(poll_ms=100.0, commit_ms=200.0, batch_size=500, max_batch_size=1000)
        assert bm._period_max_proximity == pytest.approx(0.40)

        # Third: loop = 30 + 100 + 20 = 150 → 0.15 (not a new max)
        bm.record_loop_timing(poll_ms=30.0, commit_ms=20.0, batch_size=500, max_batch_size=1000)
        assert bm._period_max_proximity == pytest.approx(0.40)  # unchanged

    def test_proximity_uses_last_batch_total(self):
        """Proximity incorporates _last_batch_total_ms from process_batch."""
        bm = _make_batch_metrics(max_poll_interval_ms=10000)
        # process_batch stores total_ms
        bm.process_batch([{"event": "test"}])
        assert bm._last_batch_total_ms > 0
        saved_batch_ms = bm._last_batch_total_ms

        bm.record_loop_timing(poll_ms=10.0, commit_ms=5.0, batch_size=1, max_batch_size=1000)
        expected = (10.0 + saved_batch_ms + 5.0) / 10000
        assert bm._cum_max_proximity == pytest.approx(expected, abs=0.001)

    def test_proximity_zero_when_no_loop_timing(self):
        """Before any record_loop_timing calls, proximity is 0."""
        bm = _make_batch_metrics()
        assert bm._period_max_proximity == 0.0
        assert bm._cum_max_proximity == 0.0

    def test_proximity_safe_with_zero_max_poll(self):
        """Zero max_poll_interval_ms doesn't compute proximity (no division by zero)."""
        bm = _make_batch_metrics(max_poll_interval_ms=0)
        bm._last_batch_total_ms = 100.0
        bm.record_loop_timing(poll_ms=50.0, commit_ms=50.0, batch_size=500, max_batch_size=1000)
        assert bm._period_max_proximity == 0.0
        assert bm._cum_max_proximity == 0.0


class TestProximityThresholdWarnings:
    """Tests for WARNING and CRITICAL log messages at proximity thresholds."""

    def test_warning_at_0_7(self, caplog):
        """Proximity > 0.7 logs a WARNING."""
        bm = _make_batch_metrics(max_poll_interval_ms=1000)
        bm._last_batch_total_ms = 600.0
        # loop = 100 + 600 + 50 = 750 → 0.75
        with caplog.at_level(logging.WARNING):
            bm.record_loop_timing(
                poll_ms=100.0, commit_ms=50.0, batch_size=500, max_batch_size=1000
            )

        warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning_msgs) == 1
        assert "0.75" in warning_msgs[0].message
        assert "nearing eviction" in warning_msgs[0].message

    def test_critical_at_0_9(self, caplog):
        """Proximity > 0.9 logs a CRITICAL (not WARNING)."""
        bm = _make_batch_metrics(max_poll_interval_ms=1000)
        bm._last_batch_total_ms = 800.0
        # loop = 100 + 800 + 50 = 950 → 0.95
        with caplog.at_level(logging.WARNING):
            bm.record_loop_timing(
                poll_ms=100.0, commit_ms=50.0, batch_size=500, max_batch_size=1000
            )

        critical_msgs = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(critical_msgs) == 1
        assert len(warning_msgs) == 0  # should be CRITICAL, not WARNING
        assert "0.95" in critical_msgs[0].message
        assert "may be evicted" in critical_msgs[0].message

    def test_no_warning_below_0_7(self, caplog):
        """Proximity <= 0.7 produces no warning or critical logs."""
        bm = _make_batch_metrics(max_poll_interval_ms=1000)
        bm._last_batch_total_ms = 400.0
        # loop = 100 + 400 + 50 = 550 → 0.55
        with caplog.at_level(logging.WARNING):
            bm.record_loop_timing(
                poll_ms=100.0, commit_ms=50.0, batch_size=500, max_batch_size=1000
            )

        warning_or_above = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_or_above) == 0

    def test_exactly_0_7_no_warning(self, caplog):
        """Proximity == 0.7 (not >) does not trigger a warning."""
        bm = _make_batch_metrics(max_poll_interval_ms=1000)
        bm._last_batch_total_ms = 500.0
        # loop = 100 + 500 + 100 = 700 → 0.70 exactly
        with caplog.at_level(logging.WARNING):
            bm.record_loop_timing(
                poll_ms=100.0, commit_ms=100.0, batch_size=500, max_batch_size=1000
            )

        warning_or_above = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_or_above) == 0


class TestProximityInSummary:
    """Tests for proximity appearing in periodic and final summaries."""

    def test_periodic_summary_includes_proximity(self, caplog):
        """Periodic loop summary line includes poll_proximity."""
        bm = _make_batch_metrics(summary_interval=0.01, max_poll_interval_ms=1000)

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=50.0, commit_ms=30.0, batch_size=800, max_batch_size=1000)

        time.sleep(0.02)

        with caplog.at_level(logging.INFO):
            bm.process_batch([{"event": "test2"}])

        loop_msgs = [r.message for r in caplog.records if "poll_proximity=" in r.message]
        assert len(loop_msgs) >= 1

    def test_final_summary_includes_proximity(self, caplog):
        """Final loop summary line includes max_poll_proximity."""
        bm = _make_batch_metrics(max_poll_interval_ms=1000)

        bm.process_batch([{"event": "test"}])
        bm._last_batch_total_ms = 200.0
        bm.record_loop_timing(poll_ms=50.0, commit_ms=30.0, batch_size=800, max_batch_size=1000)

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        loop_msgs = [r.message for r in caplog.records if "max_poll_proximity=" in r.message]
        assert len(loop_msgs) == 1

    def test_period_proximity_resets_after_summary(self):
        """Period max proximity resets to 0 after summary."""
        bm = _make_batch_metrics(summary_interval=0.01, max_poll_interval_ms=1000)

        bm.process_batch([{"event": "test"}])
        bm._last_batch_total_ms = 500.0
        bm.record_loop_timing(poll_ms=100.0, commit_ms=50.0, batch_size=500, max_batch_size=1000)
        # proximity = (100+500+50)/1000 = 0.65
        assert bm._period_max_proximity == pytest.approx(0.65)

        time.sleep(0.02)
        bm.process_batch([{"event": "test2"}])

        # After summary, period proximity should reset
        assert bm._period_max_proximity == 0.0
        # Cumulative should persist
        assert bm._cum_max_proximity == pytest.approx(0.65)

    def test_cum_proximity_persists_across_periods(self):
        """Cumulative max proximity keeps the highest value across periods."""
        bm = _make_batch_metrics(summary_interval=0.01, max_poll_interval_ms=1000)

        # Period 1: proximity 0.65
        bm.process_batch([{"event": "test"}])
        bm._last_batch_total_ms = 500.0
        bm.record_loop_timing(poll_ms=100.0, commit_ms=50.0, batch_size=500, max_batch_size=1000)

        time.sleep(0.02)
        bm.process_batch([{"event": "test2"}])  # triggers summary + reset

        # Period 2: lower proximity 0.30
        bm._last_batch_total_ms = 200.0
        bm.record_loop_timing(poll_ms=50.0, commit_ms=50.0, batch_size=500, max_batch_size=1000)

        # Cumulative should still be the higher value from period 1
        assert bm._cum_max_proximity == pytest.approx(0.65)
