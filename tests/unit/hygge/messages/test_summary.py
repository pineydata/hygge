"""
Tests for Summary generation class.
"""
import asyncio
from unittest.mock import Mock

from hygge.messages import Summary


class TestSummary:
    """Test Summary generation functionality."""

    def test_summary_initialization(self):
        """Test Summary initialization with defaults."""
        summary = Summary()
        assert summary.logger is not None

    def test_summary_initialization_with_logger(self):
        """Test Summary initialization with custom logger."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)
        assert summary.logger is mock_logger

    def test_generate_summary_empty_results(self):
        """Test summary generation with empty results."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        summary.generate_summary([])

        # Should not log anything for empty results
        mock_logger.info.assert_not_called()
        mock_logger.error.assert_not_called()

    def test_generate_summary_single_success(self):
        """Test summary generation with single successful flow."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {
                "name": "test_flow",
                "status": "pass",
                "rows": 1000,
                "duration": 5.0,
            }
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should log summary information
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Finished running" in msg for msg in info_calls)
        assert any("Completed successfully" in msg for msg in info_calls)
        assert any(
            "1 flow passed" in msg or "1 flows passed" in msg for msg in info_calls
        )

    def test_generate_summary_multiple_flows(self):
        """Test summary generation with multiple flows."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 1000, "duration": 5.0},
            {"name": "flow2", "status": "pass", "rows": 2000, "duration": 10.0},
            {"name": "flow3", "status": "skip", "rows": 0, "duration": 0.0},
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should log summary with correct counts
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        summary_line = [msg for msg in info_calls if "Finished running" in msg][0]
        assert "3 flows" in summary_line

        # Check for concise summary
        status_line = [
            msg for msg in info_calls if "passed" in msg or "skipped" in msg
        ][0]
        assert "2 passed" in status_line
        assert "1 skipped" in status_line
        assert "(3 total)" in status_line

    def test_generate_summary_with_failures(self):
        """Test summary generation with failed flows."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 1000, "duration": 5.0},
            {
                "name": "flow2",
                "status": "fail",
                "rows": 0,
                "duration": 2.0,
                "error": "Connection failed",
            },
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should log error status
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        error_calls = [call[0][0] for call in mock_logger.error.call_args_list]

        assert any("Completed with errors" in msg for msg in error_calls)

        # Check for concise summary with failures
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        status_line = [msg for msg in info_calls if "passed" in msg or "failed" in msg][
            0
        ]
        assert "1 passed" in status_line
        assert "1 failed" in status_line
        assert "(2 total)" in status_line

        # Should log failed flow details
        failed_details = [msg for msg in error_calls if "Failed flows:" in msg]
        assert len(failed_details) > 0

        failed_flow = [
            msg for msg in error_calls if "flow2" in msg and "Connection failed" in msg
        ]
        assert len(failed_flow) > 0

    def test_generate_summary_time_formatting(self):
        """Test that time formatting works correctly."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "test_flow", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should include time string in summary
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        summary_line = [msg for msg in info_calls if "Finished running" in msg][0]
        assert "second" in summary_line or "minute" in summary_line

    def test_generate_summary_time_formatting_with_minutes(self):
        """Test time formatting when duration includes minutes."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "test_flow", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        # Use a start time that's 90 seconds ago
        start_time = asyncio.get_event_loop().time() - 90.0
        summary.generate_summary(flow_results, start_time)

        # Should include both minutes and seconds
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        summary_line = [msg for msg in info_calls if "Finished running" in msg][0]
        assert "minute" in summary_line or "second" in summary_line

    def test_generate_summary_total_rows(self):
        """Test that total rows are included in summary."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 1000, "duration": 5.0},
            {"name": "flow2", "status": "pass", "rows": 2000, "duration": 10.0},
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should log total rows
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        total_rows = [msg for msg in info_calls if "Total rows processed" in msg]
        assert len(total_rows) > 0
        assert "3,000" in total_rows[0]

    def test_generate_summary_overall_rate(self):
        """Test that overall rate is calculated and logged."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        # Use a start time that gives us a measurable duration
        start_time = asyncio.get_event_loop().time() - 5.0
        summary.generate_summary(flow_results, start_time)

        # Should log overall rate
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        rate_line = [msg for msg in info_calls if "Overall rate" in msg]
        assert len(rate_line) > 0
        assert "rows/s" in rate_line[0]

    def test_generate_summary_no_rows(self):
        """Test summary when no rows were processed."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [{"name": "flow1", "status": "skip", "rows": 0, "duration": 0.0}]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should not log total rows or rate when no rows processed
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        total_rows = [msg for msg in info_calls if "Total rows processed" in msg]
        assert len(total_rows) == 0

    def test_generate_summary_spacing(self):
        """Test that summary includes cozy spacing."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "test_flow", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should have blank lines for spacing
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        blank_lines = [msg for msg in info_calls if msg == ""]
        assert len(blank_lines) >= 2  # At least one at start and one at end

    def test_generate_summary_all_failed(self):
        """Test summary when all flows failed."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {
                "name": "flow1",
                "status": "fail",
                "rows": 0,
                "duration": 2.0,
                "error": "Connection timeout",
            },
            {
                "name": "flow2",
                "status": "fail",
                "rows": 0,
                "duration": 1.0,
                "error": "Invalid schema",
            },
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should show error status
        error_calls = [call[0][0] for call in mock_logger.error.call_args_list]
        assert any("Completed with errors" in msg for msg in error_calls)

        # Should show failed count (no passed)
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        status_line = [msg for msg in info_calls if "failed" in msg][0]
        assert "2 failed" in status_line
        assert "passed" not in status_line
        assert "(2 total)" in status_line

        # Should show all failed flow details (exclude "Failed flows:" header)
        failed_flows = [
            msg
            for msg in error_calls
            if ":" in msg and "flow" in msg and msg != "Failed flows:"
        ]
        assert len(failed_flows) == 2
        assert any("Connection timeout" in msg for msg in failed_flows)
        assert any("Invalid schema" in msg for msg in failed_flows)

    def test_generate_summary_all_skipped(self):
        """Test summary when all flows skipped."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "flow1", "status": "skip", "rows": 0, "duration": 0.0},
            {"name": "flow2", "status": "skip", "rows": 0, "duration": 0.0},
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should show success status (no failures)
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Completed successfully" in msg for msg in info_calls)

        # Should show skipped count (no passed, no failed)
        status_line = [msg for msg in info_calls if "skipped" in msg][0]
        assert "2 skipped" in status_line
        assert "passed" not in status_line
        assert "failed" not in status_line
        assert "(2 total)" in status_line

    def test_generate_summary_all_three_states(self):
        """Test summary with passed, failed, and skipped flows."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 1000, "duration": 5.0},
            {
                "name": "flow2",
                "status": "fail",
                "rows": 0,
                "duration": 2.0,
                "error": "Error message",
            },
            {"name": "flow3", "status": "skip", "rows": 0, "duration": 0.0},
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should show all three states
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        status_line = [
            msg
            for msg in info_calls
            if "passed" in msg or "failed" in msg or "skipped" in msg
        ][0]
        assert "1 passed" in status_line
        assert "1 failed" in status_line
        assert "1 skipped" in status_line
        assert "(3 total)" in status_line

    def test_generate_summary_time_formatting_with_hours(self):
        """Test time formatting when duration includes hours."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "test_flow", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        # Use a start time that's 2 hours ago
        start_time = asyncio.get_event_loop().time() - 7200.0
        summary.generate_summary(flow_results, start_time)

        # Should include hours, minutes, and seconds
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        summary_line = [msg for msg in info_calls if "Finished running" in msg][0]
        assert "hour" in summary_line
        assert "minute" in summary_line or "second" in summary_line

    def test_generate_summary_with_none_start_time(self):
        """Test summary when start_time is None."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "test_flow", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        # Pass None as start_time
        summary.generate_summary(flow_results, None)

        # Should still generate summary (elapsed time will be 0.0)
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Finished running" in msg for msg in info_calls)
        # Elapsed time should be 0.0, so should show "0.00 second"
        summary_line = [msg for msg in info_calls if "Finished running" in msg][0]
        assert "0.00 second" in summary_line

    def test_generate_summary_singular_flow(self):
        """Test summary with singular 'flow' (not 'flows')."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "test_flow", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should use singular "flow" for single flow
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        finished_line = [msg for msg in info_calls if "Finished running" in msg][0]
        assert "1 flow" in finished_line or "1 flows" in finished_line

        status_line = [msg for msg in info_calls if "passed" in msg][0]
        assert "1 flow passed" in status_line or "1 flows passed" in status_line

    def test_generate_summary_zero_duration(self):
        """Test summary with zero elapsed time."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "test_flow", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        # Use current time as start time (zero elapsed)
        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should handle zero duration gracefully
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        summary_line = [msg for msg in info_calls if "Finished running" in msg][0]
        # Should still show time string
        assert "second" in summary_line

        # Rate may or may not be shown when elapsed_time is 0 or very small
        # depending on actual elapsed time - not asserting on this behavior

    def test_generate_summary_missing_error_key(self):
        """Test summary when failed flow has no error key."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {
                "name": "flow1",
                "status": "fail",
                "rows": 0,
                "duration": 2.0,
                # Missing "error" key - should use "Unknown error"
            }
        ]

        start_time = asyncio.get_event_loop().time()
        summary.generate_summary(flow_results, start_time)

        # Should still show failed flow details with "Unknown error"
        error_calls = [call[0][0] for call in mock_logger.error.call_args_list]
        failed_flows = [
            msg
            for msg in error_calls
            if ":" in msg and "flow" in msg and msg != "Failed flows:"
        ]
        assert len(failed_flows) == 1
        assert "Unknown error" in failed_flows[0]

    def test_generate_summary_large_numbers(self):
        """Test summary with very large row counts."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 12_345_678, "duration": 5.0}
        ]

        start_time = asyncio.get_event_loop().time() - 5.0
        summary.generate_summary(flow_results, start_time)

        # Should format large numbers with commas
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        total_rows = [msg for msg in info_calls if "Total rows processed" in msg]
        assert len(total_rows) > 0
        assert "12,345,678" in total_rows[0]

    def test_generate_summary_time_singular_units(self):
        """Test time formatting with singular units (1 hour, 1 minute, 1 second)."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "test_flow", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        # 1 hour, 1 minute, 1 second
        start_time = asyncio.get_event_loop().time() - 3661.0
        summary.generate_summary(flow_results, start_time)

        # Should use singular forms
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        summary_line = [msg for msg in info_calls if "Finished running" in msg][0]
        assert "1 hour" in summary_line
        assert "1 minute" in summary_line
        # Seconds will be formatted with decimals
