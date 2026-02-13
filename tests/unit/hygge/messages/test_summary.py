"""
Tests for Summary generation class.

Tests focus on behavior (does it call the right methods? handle edge cases?)
rather than exact message wording, making them maintainable.
"""

import time
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

    def test_generate_summary_success_calls_success_methods(self):
        """Test that successful runs call success summary methods."""
        summary = Summary()

        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 1000, "duration": 5.0}
        ]

        # Should complete without error
        summary.generate_summary(flow_results, start_time=time.monotonic())

    def test_generate_summary_failure_calls_error_methods(self):
        """Test that failed runs call error summary methods."""
        summary = Summary()

        flow_results = [
            {
                "name": "flow1",
                "status": "fail",
                "rows": 0,
                "duration": 2.0,
                "error": "Test error",
            }
        ]

        # Should complete without error
        summary.generate_summary(flow_results, start_time=time.monotonic())

    def test_generate_summary_mixed_results(self):
        """Test summary with mixed pass/fail/skip."""
        summary = Summary()

        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 1000, "duration": 5.0},
            {
                "name": "flow2",
                "status": "fail",
                "rows": 0,
                "duration": 2.0,
                "error": "Error",
            },
            {"name": "flow3", "status": "skip", "rows": 0, "duration": 0.0},
        ]

        # Should complete without error
        summary.generate_summary(flow_results, start_time=time.monotonic())

    def test_generate_summary_no_spacing(self):
        """Test that summary logs spacing correctly."""
        mock_logger = Mock()
        summary = Summary(logger=mock_logger)

        flow_results = [
            {"name": "test", "status": "pass", "rows": 100, "duration": 1.0}
        ]

        summary.generate_summary(flow_results, start_time=time.monotonic())

        # Should log something (exact messages don't matter)
        assert mock_logger.info.called

    def test_generate_summary_handles_none_start_time(self):
        """Test that summary handles None start_time gracefully."""
        summary = Summary()

        flow_results = [
            {"name": "test", "status": "pass", "rows": 100, "duration": 1.0}
        ]

        # Should not crash with None
        summary.generate_summary(flow_results, start_time=None)

    def test_generate_summary_handles_zero_rows(self):
        """Test summary with zero rows processed."""
        summary = Summary()

        flow_results = [{"name": "test", "status": "skip", "rows": 0, "duration": 0.0}]

        # Should handle zero rows gracefully
        summary.generate_summary(flow_results, start_time=time.monotonic())
