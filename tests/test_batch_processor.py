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

from clickstream.consumers.batch_processor import BatchMetrics, _linear_slope, _precision

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


# ==============================================================================
# Idle time (commit-to-poll gap) — Phase 4c
# ==============================================================================


class TestIdleTimeAccumulation:
    """Tests for idle_ms accumulation in record_loop_timing."""

    def test_idle_ms_accumulates_period_and_cumulative(self):
        """idle_ms is accumulated into both period and cumulative counters."""
        bm = _make_batch_metrics()
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000, idle_ms=10.0
        )
        assert bm._period_idle_ms == 10.0
        assert bm._cum_idle_ms == 10.0

    def test_multiple_idle_ms_accumulate(self):
        """Multiple calls accumulate idle_ms correctly."""
        bm = _make_batch_metrics()
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000, idle_ms=10.0
        )
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000, idle_ms=25.0
        )
        assert bm._period_idle_ms == 35.0
        assert bm._cum_idle_ms == 35.0

    def test_idle_ms_defaults_to_zero(self):
        """When idle_ms is not passed, it defaults to 0."""
        bm = _make_batch_metrics()
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)
        assert bm._period_idle_ms == 0.0
        assert bm._cum_idle_ms == 0.0

    def test_idle_ms_starts_at_zero(self):
        """Idle time fields start at zero before any recording."""
        bm = _make_batch_metrics()
        assert bm._period_idle_ms == 0.0
        assert bm._cum_idle_ms == 0.0


class TestIdleTimePeriodReset:
    """Tests for idle_ms period reset after summary."""

    def test_period_idle_resets_after_summary(self):
        """Period idle_ms resets to 0 after summary, cumulative persists."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000, idle_ms=15.0
        )

        time.sleep(0.02)
        bm.process_batch([{"event": "test2"}])  # triggers summary + reset

        assert bm._period_idle_ms == 0.0
        assert bm._cum_idle_ms == 15.0

    def test_cumulative_idle_persists_across_periods(self):
        """Cumulative idle_ms accumulates across multiple periods."""
        bm = _make_batch_metrics(summary_interval=0.01)

        # Period 1
        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000, idle_ms=10.0
        )

        time.sleep(0.02)
        bm.process_batch([{"event": "test2"}])  # triggers summary + reset

        # Period 2
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000, idle_ms=20.0
        )

        assert bm._period_idle_ms == 20.0
        assert bm._cum_idle_ms == 30.0


class TestHeadroomCalculation:
    """Tests for headroom percentage calculation in summaries."""

    def test_headroom_in_periodic_summary(self, caplog):
        """Periodic summary includes headroom percentage."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.process_batch([{"event": "test"}])
        # idle=20ms, poll=40ms, commit=40ms → avg_loop = 40+40+20 = 100ms
        # headroom = 20/100 * 100 = 20%
        bm.record_loop_timing(
            poll_ms=40.0, commit_ms=40.0, batch_size=800, max_batch_size=1000, idle_ms=20.0
        )

        time.sleep(0.02)

        with caplog.at_level(logging.INFO):
            bm.process_batch([{"event": "test2"}])

        loop_msgs = [r.message for r in caplog.records if "headroom:" in r.message]
        assert len(loop_msgs) >= 1
        assert "headroom: 20%" in loop_msgs[0]

    def test_headroom_zero_when_no_idle(self, caplog):
        """Headroom is 0% when idle_ms is 0."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(
            poll_ms=50.0, commit_ms=50.0, batch_size=800, max_batch_size=1000, idle_ms=0.0
        )

        time.sleep(0.02)

        with caplog.at_level(logging.INFO):
            bm.process_batch([{"event": "test2"}])

        loop_msgs = [r.message for r in caplog.records if "headroom:" in r.message]
        assert len(loop_msgs) >= 1
        assert "headroom: 0%" in loop_msgs[0]

    def test_headroom_in_final_summary(self, caplog):
        """Final summary includes headroom percentage."""
        bm = _make_batch_metrics()

        bm.process_batch([{"event": "test"}])
        # idle=50ms, poll=25ms, commit=25ms → avg_loop = 25+25+50 = 100ms
        # headroom = 50/100 * 100 = 50%
        bm.record_loop_timing(
            poll_ms=25.0, commit_ms=25.0, batch_size=800, max_batch_size=1000, idle_ms=50.0
        )

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        loop_msgs = [r.message for r in caplog.records if "Final loop:" in r.message]
        assert len(loop_msgs) == 1
        assert "headroom: 50%" in loop_msgs[0]

    def test_avg_idle_in_periodic_summary(self, caplog):
        """Periodic summary shows avg_idle value."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(
            poll_ms=10.0, commit_ms=5.0, batch_size=800, max_batch_size=1000, idle_ms=8.0
        )
        bm.record_loop_timing(
            poll_ms=10.0, commit_ms=5.0, batch_size=800, max_batch_size=1000, idle_ms=12.0
        )

        time.sleep(0.02)

        with caplog.at_level(logging.INFO):
            bm.process_batch([{"event": "test2"}])

        loop_msgs = [r.message for r in caplog.records if "avg_idle=" in r.message]
        assert len(loop_msgs) >= 1

    def test_avg_idle_in_final_summary(self, caplog):
        """Final summary shows avg_idle value."""
        bm = _make_batch_metrics()

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(
            poll_ms=10.0, commit_ms=5.0, batch_size=800, max_batch_size=1000, idle_ms=6.0
        )
        bm.record_loop_timing(
            poll_ms=10.0, commit_ms=5.0, batch_size=800, max_batch_size=1000, idle_ms=14.0
        )

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        loop_msgs = [r.message for r in caplog.records if "Final loop:" in r.message]
        assert len(loop_msgs) == 1
        assert "avg_idle=" in loop_msgs[0]


# ==============================================================================
# Rebalance tracking — Phase 4d
# ==============================================================================


class TestRecordRebalance:
    """Tests for the record_rebalance method."""

    def test_record_rebalance_appends_timestamp(self):
        """record_rebalance() appends a monotonic timestamp."""
        bm = _make_batch_metrics()
        assert len(bm._rebalance_events) == 0

        bm.record_rebalance()
        assert len(bm._rebalance_events) == 1
        assert isinstance(bm._rebalance_events[0], float)

    def test_multiple_rebalances_accumulate(self):
        """Multiple record_rebalance() calls accumulate timestamps."""
        bm = _make_batch_metrics()
        bm.record_rebalance()
        bm.record_rebalance()
        bm.record_rebalance()
        assert len(bm._rebalance_events) == 3

    def test_rebalance_timestamps_are_monotonically_increasing(self):
        """Recorded timestamps are monotonically increasing."""
        bm = _make_batch_metrics()
        bm.record_rebalance()
        bm.record_rebalance()
        assert bm._rebalance_events[1] >= bm._rebalance_events[0]

    def test_rebalance_events_starts_empty(self):
        """_rebalance_events list starts empty before any recording."""
        bm = _make_batch_metrics()
        assert bm._rebalance_events == []


class TestRebalancePeriodicSummary:
    """Tests for rebalance count in periodic summaries."""

    def test_no_warning_when_zero_rebalances(self, caplog):
        """No rebalance warning is logged when there are no rebalances."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        time.sleep(0.02)

        with caplog.at_level(logging.WARNING):
            bm.process_batch([{"event": "test2"}])

        warning_msgs = [
            r.message for r in caplog.records if "rebalances in last 5 minutes" in r.message
        ]
        assert len(warning_msgs) == 0

    def test_no_warning_when_two_rebalances(self, caplog):
        """No warning when exactly 2 rebalances in 5 minutes (threshold is >2)."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.record_rebalance()
        bm.record_rebalance()

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        time.sleep(0.02)

        with caplog.at_level(logging.WARNING):
            bm.process_batch([{"event": "test2"}])

        warning_msgs = [
            r.message for r in caplog.records if "rebalances in last 5 minutes" in r.message
        ]
        assert len(warning_msgs) == 0

    def test_warning_when_three_rebalances(self, caplog):
        """WARNING logged when >2 rebalances occur in 5 minutes."""
        bm = _make_batch_metrics(summary_interval=0.01)

        bm.record_rebalance()
        bm.record_rebalance()
        bm.record_rebalance()

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        time.sleep(0.02)

        with caplog.at_level(logging.WARNING):
            bm.process_batch([{"event": "test2"}])

        warning_msgs = [
            r.message for r in caplog.records if "rebalances in last 5 minutes" in r.message
        ]
        assert len(warning_msgs) == 1
        assert "3 rebalances" in warning_msgs[0]
        assert "consumer group unstable" in warning_msgs[0]

    def test_warning_includes_correct_count(self, caplog):
        """Warning message includes the exact count of recent rebalances."""
        bm = _make_batch_metrics(summary_interval=0.01)

        for _ in range(5):
            bm.record_rebalance()

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        time.sleep(0.02)

        with caplog.at_level(logging.WARNING):
            bm.process_batch([{"event": "test2"}])

        warning_msgs = [
            r.message for r in caplog.records if "rebalances in last 5 minutes" in r.message
        ]
        assert len(warning_msgs) == 1
        assert "5 rebalances" in warning_msgs[0]

    def test_old_rebalances_not_counted(self, caplog):
        """Rebalances older than 5 minutes don't count toward the warning."""
        bm = _make_batch_metrics(summary_interval=0.01)

        # Simulate 3 old rebalances by directly injecting timestamps
        now = time.monotonic()
        bm._rebalance_events = [now - 400, now - 350, now - 310]  # All > 5 min ago

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        time.sleep(0.02)

        with caplog.at_level(logging.WARNING):
            bm.process_batch([{"event": "test2"}])

        warning_msgs = [
            r.message for r in caplog.records if "rebalances in last 5 minutes" in r.message
        ]
        assert len(warning_msgs) == 0

    def test_mix_of_old_and_recent_rebalances(self, caplog):
        """Only recent rebalances count; old ones are excluded."""
        bm = _make_batch_metrics(summary_interval=0.01)

        now = time.monotonic()
        # 2 old rebalances (don't count) + 3 recent ones (do count)
        bm._rebalance_events = [now - 400, now - 350, now - 1, now - 0.5, now - 0.1]

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        time.sleep(0.02)

        with caplog.at_level(logging.WARNING):
            bm.process_batch([{"event": "test2"}])

        warning_msgs = [
            r.message for r in caplog.records if "rebalances in last 5 minutes" in r.message
        ]
        assert len(warning_msgs) == 1
        assert "3 rebalances" in warning_msgs[0]


class TestRebalanceFinalSummary:
    """Tests for rebalance count in final summary."""

    def test_final_summary_includes_rebalance_count(self, caplog):
        """Final summary includes total rebalance count."""
        bm = _make_batch_metrics()

        bm.record_rebalance()
        bm.record_rebalance()

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        loop_msgs = [r.message for r in caplog.records if "Final loop:" in r.message]
        assert len(loop_msgs) == 1
        assert "rebalances=2" in loop_msgs[0]

    def test_final_summary_zero_rebalances(self, caplog):
        """Final summary shows rebalances=0 when none occurred."""
        bm = _make_batch_metrics()

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        loop_msgs = [r.message for r in caplog.records if "Final loop:" in r.message]
        assert len(loop_msgs) == 1
        assert "rebalances=0" in loop_msgs[0]

    def test_final_summary_counts_all_rebalances(self, caplog):
        """Final summary counts ALL rebalances, including ones older than 5 min."""
        bm = _make_batch_metrics()

        # Inject old rebalance + record new ones
        now = time.monotonic()
        bm._rebalance_events = [now - 400]  # 1 old
        bm.record_rebalance()  # 1 recent
        bm.record_rebalance()  # 1 recent

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        with caplog.at_level(logging.INFO):
            bm.log_final_summary()

        loop_msgs = [r.message for r in caplog.records if "Final loop:" in r.message]
        assert len(loop_msgs) == 1
        # Total = 3 (1 old + 2 recent)
        assert "rebalances=3" in loop_msgs[0]


# ==============================================================================
# _linear_slope helper — Phase 4e
# ==============================================================================


class TestLinearSlope:
    """Tests for the _linear_slope helper function."""

    def test_empty_list_returns_zero(self):
        assert _linear_slope([]) == 0.0

    def test_single_value_returns_zero(self):
        assert _linear_slope([5.0]) == 0.0

    def test_constant_values_zero_slope(self):
        """All identical values should give slope ≈ 0."""
        assert _linear_slope([10.0, 10.0, 10.0, 10.0]) == pytest.approx(0.0)

    def test_linearly_increasing(self):
        """Perfectly linear increase: y = 2x → slope = 2."""
        values = [0.0, 2.0, 4.0, 6.0, 8.0]
        assert _linear_slope(values) == pytest.approx(2.0)

    def test_linearly_decreasing(self):
        """Perfectly linear decrease: y = 10 - x → slope = -1."""
        values = [10.0, 9.0, 8.0, 7.0, 6.0]
        assert _linear_slope(values) == pytest.approx(-1.0)

    def test_two_values_positive(self):
        """Two values: slope = difference."""
        assert _linear_slope([3.0, 7.0]) == pytest.approx(4.0)

    def test_two_values_negative(self):
        """Two values: slope = difference (negative)."""
        assert _linear_slope([7.0, 3.0]) == pytest.approx(-4.0)

    def test_noisy_increasing_positive_slope(self):
        """Noisy but generally increasing values → positive slope."""
        values = [1.0, 3.0, 2.0, 5.0, 4.0, 7.0, 6.0, 9.0]
        slope = _linear_slope(values)
        assert slope > 0.5


# ==============================================================================
# Stage latency trends — Phase 4e
# ==============================================================================


class TestStageWindowAccumulation:
    """Tests for stage window data collection in process_batch."""

    def test_stage_window_starts_empty(self):
        """Stage window deque starts empty."""
        bm = _make_batch_metrics()
        assert len(bm._stage_window) == 0

    def test_process_batch_appends_to_window(self):
        """Each process_batch appends stage timings to the window."""
        bm = _make_batch_metrics()
        bm.process_batch([{"event": "test1"}])
        assert len(bm._stage_window) == 1
        entry = bm._stage_window[0]
        assert "pg_events_ms" in entry
        assert "valkey_ms" in entry
        assert "pg_sessions_ms" in entry

    def test_window_maxlen_100(self):
        """Stage window is bounded at 100 entries."""
        bm = _make_batch_metrics()
        for i in range(110):
            bm.process_batch([{"event": f"test{i}"}])
        assert len(bm._stage_window) == 100

    def test_window_values_are_positive(self):
        """Stage timing values are non-negative floats."""
        bm = _make_batch_metrics()
        bm.process_batch([{"event": "test"}])
        entry = bm._stage_window[0]
        for key in ("pg_events_ms", "valkey_ms", "pg_sessions_ms"):
            assert isinstance(entry[key], float)
            assert entry[key] >= 0


class TestComputeStageTrends:
    """Tests for _compute_stage_trends method."""

    def test_empty_window_returns_no_trends(self):
        """No trends when window is empty."""
        bm = _make_batch_metrics()
        assert bm._compute_stage_trends() == []

    def test_single_entry_returns_no_trends(self):
        """No trends with only one sample (need >= 2)."""
        bm = _make_batch_metrics()
        bm._stage_window.append({"pg_events_ms": 5.0, "valkey_ms": 3.0, "pg_sessions_ms": 4.0})
        assert bm._compute_stage_trends() == []

    def test_stable_stages_no_trends(self):
        """Constant stage timings → no trending warnings."""
        bm = _make_batch_metrics()
        for _ in range(10):
            bm._stage_window.append({"pg_events_ms": 5.0, "valkey_ms": 3.0, "pg_sessions_ms": 4.0})
        assert bm._compute_stage_trends() == []

    def test_valkey_trending_up(self):
        """Valkey increasing by 1ms/batch → slope=1.0 > 0.5 threshold."""
        bm = _make_batch_metrics()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0, "valkey_ms": 3.0 + i, "pg_sessions_ms": 4.0}
            )
        trends = bm._compute_stage_trends()
        assert len(trends) == 1
        assert "valkey trending up" in trends[0]

    def test_pg_events_trending_up(self):
        """pg_events increasing → flagged."""
        bm = _make_batch_metrics()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0 + i * 2, "valkey_ms": 3.0, "pg_sessions_ms": 4.0}
            )
        trends = bm._compute_stage_trends()
        assert len(trends) == 1
        assert "pg_events trending up" in trends[0]

    def test_pg_sessions_trending_up(self):
        """pg_sessions increasing → flagged."""
        bm = _make_batch_metrics()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0, "valkey_ms": 3.0, "pg_sessions_ms": 4.0 + i}
            )
        trends = bm._compute_stage_trends()
        assert len(trends) == 1
        assert "pg_sessions trending up" in trends[0]

    def test_multiple_stages_trending_up(self):
        """Multiple stages can trend up simultaneously."""
        bm = _make_batch_metrics()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0 + i, "valkey_ms": 3.0 + i, "pg_sessions_ms": 4.0}
            )
        trends = bm._compute_stage_trends()
        assert len(trends) == 2
        labels = " ".join(trends)
        assert "pg_events trending up" in labels
        assert "valkey trending up" in labels

    def test_decreasing_stage_not_flagged(self):
        """A stage with decreasing latency should not be flagged."""
        bm = _make_batch_metrics()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 20.0 - i, "valkey_ms": 3.0, "pg_sessions_ms": 4.0}
            )
        assert bm._compute_stage_trends() == []

    def test_slope_below_threshold_not_flagged(self):
        """Slope of exactly 0.5 should not be flagged (threshold is > 0.5)."""
        bm = _make_batch_metrics()
        # slope = 0.5 ms/batch
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0, "valkey_ms": 3.0 + i * 0.5, "pg_sessions_ms": 4.0}
            )
        trends = bm._compute_stage_trends()
        assert len(trends) == 0

    def test_trend_includes_first_and_last_ms(self):
        """Trend message includes first and last timing values."""
        bm = _make_batch_metrics()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0, "valkey_ms": 3.0 + i, "pg_sessions_ms": 4.0}
            )
        trends = bm._compute_stage_trends()
        assert len(trends) == 1
        # First value: 3ms, last value: 12ms
        assert "3ms" in trends[0]
        assert "12ms" in trends[0]


class TestStageTrendsInPeriodicSummary:
    """Tests for stage trend warnings in periodic summaries."""

    def test_no_trend_warning_when_stable(self, caplog):
        """No 'Stage trends' warning when all stages are stable."""
        bm = _make_batch_metrics(summary_interval=0.01)

        # Process enough batches with constant timings
        for _ in range(5):
            bm.process_batch([{"event": "test"}])

        time.sleep(0.02)

        with caplog.at_level(logging.WARNING):
            bm.process_batch([{"event": "trigger"}])

        trend_msgs = [r.message for r in caplog.records if "Stage trends:" in r.message]
        assert len(trend_msgs) == 0

    def test_trend_warning_when_stage_increasing(self, caplog):
        """'Stage trends' WARNING logged when a stage latency is increasing."""
        bm = _make_batch_metrics(summary_interval=0.01)

        # First batch to initialize period counters
        bm.process_batch([{"event": "test"}])

        # Clear the window and inject increasing valkey timings
        bm._stage_window.clear()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0, "valkey_ms": 3.0 + i * 2, "pg_sessions_ms": 4.0}
            )

        time.sleep(0.02)

        with caplog.at_level(logging.WARNING):
            # This appends one more entry but the slope is still strong
            bm.process_batch([{"event": "trigger"}])

        trend_msgs = [r.message for r in caplog.records if "Stage trends:" in r.message]
        assert len(trend_msgs) == 1
        assert "valkey trending up" in trend_msgs[0]


class TestStageTrendsInFinalSummary:
    """Tests for stage trend warnings in final summary."""

    def test_no_trend_warning_when_stable(self, caplog):
        """No 'Final stage trends' warning when all stages are stable."""
        bm = _make_batch_metrics()

        for _ in range(5):
            bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        with caplog.at_level(logging.WARNING):
            bm.log_final_summary()

        trend_msgs = [r.message for r in caplog.records if "Final stage trends:" in r.message]
        assert len(trend_msgs) == 0

    def test_trend_warning_in_final_summary(self, caplog):
        """'Final stage trends' WARNING logged when a stage latency is increasing."""
        bm = _make_batch_metrics()

        # Inject increasing pg_events timings
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0 + i * 3, "valkey_ms": 3.0, "pg_sessions_ms": 4.0}
            )

        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)

        with caplog.at_level(logging.WARNING):
            bm.log_final_summary()

        trend_msgs = [r.message for r in caplog.records if "Final stage trends:" in r.message]
        assert len(trend_msgs) == 1
        assert "pg_events trending up" in trend_msgs[0]


# ==============================================================================
# Backpressure indicators — Follow-up wiring
# ==============================================================================


class TestGetBackpressureIndicators:
    """Tests for get_backpressure_indicators() method."""

    def test_returns_all_expected_keys(self):
        """Returned dict contains all 5 expected indicator keys."""
        bm = _make_batch_metrics()
        indicators = bm.get_backpressure_indicators()
        assert set(indicators.keys()) == {
            "fill_ratio",
            "poll_proximity",
            "idle_ms",
            "rebalance_count",
            "bottleneck_stage",
        }

    def test_defaults_when_no_data(self):
        """All indicators default to zero/None when no data has been recorded."""
        bm = _make_batch_metrics()
        indicators = bm.get_backpressure_indicators()
        assert indicators["fill_ratio"] == 0.0
        assert indicators["poll_proximity"] == 0.0
        assert indicators["idle_ms"] == 0.0
        assert indicators["rebalance_count"] == 0
        assert indicators["bottleneck_stage"] is None

    def test_fill_ratio_average(self):
        """fill_ratio is the average of period fill ratios."""
        bm = _make_batch_metrics()
        bm.process_batch([{"event": "test"}])
        # Two loops with different fill ratios: 0.8 and 0.6 → avg = 0.7
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000)
        bm.record_loop_timing(poll_ms=5.0, commit_ms=3.0, batch_size=600, max_batch_size=1000)

        indicators = bm.get_backpressure_indicators()
        assert indicators["fill_ratio"] == pytest.approx(0.7)

    def test_poll_proximity_is_period_max(self):
        """poll_proximity returns the max proximity seen in the current period."""
        bm = _make_batch_metrics(max_poll_interval_ms=1000)
        bm.process_batch([{"event": "test"}])
        # poll + batch + commit = 5 + ~0 + 3 = ~8ms → proximity ≈ 8/1000
        # But _last_batch_total_ms is set by process_batch
        bm.record_loop_timing(poll_ms=500.0, commit_ms=200.0, batch_size=800, max_batch_size=1000)

        indicators = bm.get_backpressure_indicators()
        # proximity = (500 + batch_ms + 200) / 1000 — should be > 0.7
        assert indicators["poll_proximity"] > 0.0
        assert indicators["poll_proximity"] == bm._period_max_proximity

    def test_idle_ms_average(self):
        """idle_ms is the average idle time across loops in the period."""
        bm = _make_batch_metrics()
        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000, idle_ms=10.0
        )
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=800, max_batch_size=1000, idle_ms=30.0
        )

        indicators = bm.get_backpressure_indicators()
        assert indicators["idle_ms"] == pytest.approx(20.0)

    def test_rebalance_count_recent_only(self):
        """rebalance_count only counts rebalances in the last 5 minutes."""
        bm = _make_batch_metrics()
        # 2 recent rebalances
        bm.record_rebalance()
        bm.record_rebalance()
        # 1 old rebalance (> 5 min ago)
        now = time.monotonic()
        bm._rebalance_events.insert(0, now - 400)

        indicators = bm.get_backpressure_indicators()
        assert indicators["rebalance_count"] == 2

    def test_bottleneck_stage_none_when_stable(self):
        """bottleneck_stage is None when no stages are trending up."""
        bm = _make_batch_metrics()
        for _ in range(10):
            bm._stage_window.append({"pg_events_ms": 5.0, "valkey_ms": 3.0, "pg_sessions_ms": 4.0})

        indicators = bm.get_backpressure_indicators()
        assert indicators["bottleneck_stage"] is None

    def test_bottleneck_stage_identifies_worst(self):
        """bottleneck_stage returns the stage with the steepest upward trend."""
        bm = _make_batch_metrics()
        # valkey increasing by 2ms/batch, pg_events by 1ms/batch
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0 + i, "valkey_ms": 3.0 + i * 2, "pg_sessions_ms": 4.0}
            )

        indicators = bm.get_backpressure_indicators()
        assert indicators["bottleneck_stage"] == "valkey"

    def test_bottleneck_stage_pg_events(self):
        """bottleneck_stage returns pg_events when it has the steepest trend."""
        bm = _make_batch_metrics()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0 + i * 3, "valkey_ms": 3.0, "pg_sessions_ms": 4.0}
            )

        indicators = bm.get_backpressure_indicators()
        assert indicators["bottleneck_stage"] == "pg_events"

    def test_bottleneck_stage_pg_sessions(self):
        """bottleneck_stage returns pg_sessions when it has the steepest trend."""
        bm = _make_batch_metrics()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0, "valkey_ms": 3.0, "pg_sessions_ms": 4.0 + i * 3}
            )

        indicators = bm.get_backpressure_indicators()
        assert indicators["bottleneck_stage"] == "pg_sessions"

    def test_indicators_reflect_period_state(self):
        """Indicators reflect current period data, not cumulative."""
        bm = _make_batch_metrics(summary_interval=0.01)

        # Period 1: fill ratio 0.9
        bm.process_batch([{"event": "test"}])
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=900, max_batch_size=1000, idle_ms=50.0
        )

        time.sleep(0.02)
        bm.process_batch([{"event": "test2"}])  # triggers summary + reset

        # Period 2: fill ratio 0.5
        bm.record_loop_timing(
            poll_ms=5.0, commit_ms=3.0, batch_size=500, max_batch_size=1000, idle_ms=10.0
        )

        indicators = bm.get_backpressure_indicators()
        assert indicators["fill_ratio"] == pytest.approx(0.5)
        assert indicators["idle_ms"] == pytest.approx(10.0)

    def test_bottleneck_stage_none_with_insufficient_window(self):
        """bottleneck_stage is None when window has fewer than 2 entries."""
        bm = _make_batch_metrics()
        bm._stage_window.append({"pg_events_ms": 5.0, "valkey_ms": 3.0, "pg_sessions_ms": 4.0})

        indicators = bm.get_backpressure_indicators()
        assert indicators["bottleneck_stage"] is None


class TestGetBottleneckStage:
    """Tests for _get_bottleneck_stage helper method."""

    def test_empty_window_returns_none(self):
        """Returns None when window is empty."""
        bm = _make_batch_metrics()
        assert bm._get_bottleneck_stage() is None

    def test_single_entry_returns_none(self):
        """Returns None with only one sample."""
        bm = _make_batch_metrics()
        bm._stage_window.append({"pg_events_ms": 5.0, "valkey_ms": 3.0, "pg_sessions_ms": 4.0})
        assert bm._get_bottleneck_stage() is None

    def test_stable_stages_returns_none(self):
        """Returns None when all stages are stable (no slope > 0.5)."""
        bm = _make_batch_metrics()
        for _ in range(10):
            bm._stage_window.append({"pg_events_ms": 5.0, "valkey_ms": 3.0, "pg_sessions_ms": 4.0})
        assert bm._get_bottleneck_stage() is None

    def test_returns_steepest_stage(self):
        """Returns the stage with the steepest slope when multiple are trending."""
        bm = _make_batch_metrics()
        # valkey slope=2, pg_events slope=1
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0 + i, "valkey_ms": 3.0 + i * 2, "pg_sessions_ms": 4.0}
            )
        assert bm._get_bottleneck_stage() == "valkey"

    def test_strips_ms_suffix(self):
        """Returned stage name has _ms suffix removed."""
        bm = _make_batch_metrics()
        for i in range(10):
            bm._stage_window.append(
                {"pg_events_ms": 5.0, "valkey_ms": 3.0, "pg_sessions_ms": 4.0 + i * 2}
            )
        result = bm._get_bottleneck_stage()
        assert result == "pg_sessions"
        assert "_ms" not in result
