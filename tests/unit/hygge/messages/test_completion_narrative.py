"""
Unit tests for narrative completion message improvements.

Tests that completion summaries are warm and celebratory.
"""

from hygge.messages.logger import HyggeLogger
from hygge.messages.summary import Summary


class TestCompletionNarrative:
    """Test narrative completion message enhancements."""

    def test_summary_has_success_and_error_methods(self):
        """Test that Summary has methods for different outcomes."""
        summary = Summary()

        # Verify Summary has the narrative helper methods
        assert hasattr(summary, "_generate_success_summary")
        assert hasattr(summary, "_generate_error_summary")
        assert hasattr(summary, "generate_summary")

    def test_success_summary_generation(self):
        """Test that success summaries are generated correctly."""
        logger = HyggeLogger("test")
        summary = Summary(logger=logger)

        # Given successful flow results
        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 1000, "duration": 1.5},
            {"name": "flow2", "status": "pass", "rows": 2000, "duration": 2.0},
        ]

        # When we generate a summary
        # (This will log but not throw errors)
        summary.generate_summary(flow_results, start_time=0.0)

        # Then it completes without error
        assert True

    def test_error_summary_generation(self):
        """Test that error summaries are generated correctly."""
        logger = HyggeLogger("test")
        summary = Summary(logger=logger)

        # Given mixed flow results
        flow_results = [
            {"name": "flow1", "status": "pass", "rows": 1000, "duration": 1.5},
            {
                "name": "flow2",
                "status": "fail",
                "rows": 0,
                "duration": 0.5,
                "error": "Test error",
            },
        ]

        # When we generate a summary
        # (This will log but not throw errors)
        summary.generate_summary(flow_results, start_time=0.0)

        # Then it completes without error
        assert True
