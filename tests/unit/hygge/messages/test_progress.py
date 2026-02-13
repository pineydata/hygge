"""
Tests for Progress tracking class.
"""

import asyncio
import time
from unittest.mock import Mock, patch

import pytest

from hygge.messages import Progress


class TestProgress:
    """Test Progress tracking functionality."""

    def test_progress_initialization(self):
        """Test Progress initialization with defaults."""
        progress = Progress()
        assert progress.total_rows_progress == 0
        assert progress.last_milestone_rows == 0
        assert progress.milestone_interval == 1_000_000
        assert progress.run_start_time is None
        assert progress.logger is not None

    def test_progress_initialization_with_custom_interval(self):
        """Test Progress initialization with custom milestone interval."""
        progress = Progress(milestone_interval=500_000)
        assert progress.milestone_interval == 500_000

    def test_progress_initialization_with_logger(self):
        """Test Progress initialization with custom logger."""
        mock_logger = Mock()
        progress = Progress(logger=mock_logger)
        assert progress.logger is mock_logger

    def test_progress_start(self):
        """Test starting progress tracking."""
        progress = Progress()
        start_time = time.monotonic()
        progress.start(start_time)

        assert progress.run_start_time == start_time
        assert progress.total_rows_progress == 0
        assert progress.last_milestone_rows == 0

    def test_progress_start_with_default_time(self):
        """Test starting progress tracking with default time."""
        progress = Progress()
        with patch("asyncio.get_event_loop") as mock_loop:
            mock_time = Mock(return_value=100.0)
            mock_loop.return_value.time = mock_time

            progress.start()

            assert progress.run_start_time == 100.0

    def test_progress_start_resets_counters(self):
        """Test that start() resets counters when called multiple times."""
        progress = Progress()
        start_time1 = time.monotonic()
        progress.start(start_time1)
        progress.total_rows_progress = 500_000
        progress.last_milestone_rows = 500_000

        # Call start again
        start_time2 = time.monotonic()
        progress.start(start_time2)

        # Should reset counters
        assert progress.run_start_time == start_time2
        assert progress.total_rows_progress == 0
        assert progress.last_milestone_rows == 0

    @pytest.mark.asyncio
    async def test_progress_update_no_milestone(self):
        """Test progress update that doesn't reach milestone."""
        progress = Progress(milestone_interval=1_000_000)
        progress.start(time.monotonic())

        with patch.object(progress.logger, "info") as mock_info:
            await progress.update(500_000)

            assert progress.total_rows_progress == 500_000
            assert progress.last_milestone_rows == 0
            mock_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_progress_update_zero_rows(self):
        """Test progress update with zero rows."""
        progress = Progress(milestone_interval=1_000_000)
        progress.start(time.monotonic())

        with patch.object(progress.logger, "info") as mock_info:
            await progress.update(0)

            assert progress.total_rows_progress == 0
            assert progress.last_milestone_rows == 0
            mock_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_progress_update_reaches_milestone(self):
        """Test progress update that reaches a milestone."""
        progress = Progress(milestone_interval=1_000_000)
        start_time = time.monotonic()
        progress.start(start_time)

        with patch.object(progress.logger, "info") as mock_info:
            # Update enough to reach first milestone
            await progress.update(1_000_000)

            assert progress.total_rows_progress == 1_000_000
            assert progress.last_milestone_rows == 1_000_000
            mock_info.assert_called_once()
            # Verify the log message format (now uses "moved" instead of "PROCESSED")
            call_args = mock_info.call_args[0][0]
            assert "Milestone" in call_args or "moved" in call_args
            assert "1,000,000" in call_args

    @pytest.mark.asyncio
    async def test_progress_update_multiple_milestones(self):
        """Test progress update that crosses multiple milestones."""
        progress = Progress(milestone_interval=1_000_000)
        start_time = time.monotonic()
        progress.start(start_time)

        with patch.object(progress.logger, "info") as mock_info:
            # Update enough to cross multiple milestones
            await progress.update(3_500_000)

            assert progress.total_rows_progress == 3_500_000
            assert progress.last_milestone_rows == 3_000_000
            # Should log 3 milestones (1M, 2M, 3M)
            assert mock_info.call_count == 3

    @pytest.mark.asyncio
    async def test_progress_update_concurrent_updates(self):
        """Test that concurrent progress updates are handled correctly."""
        progress = Progress(milestone_interval=1_000_000)
        start_time = time.monotonic()
        progress.start(start_time)

        # Create multiple concurrent updates
        async def update_rows(rows):
            await progress.update(rows)

        tasks = [update_rows(500_000) for _ in range(4)]
        await asyncio.gather(*tasks)

        # Should have 2M total rows
        assert progress.total_rows_progress == 2_000_000
        # Should have logged 2 milestones
        assert progress.last_milestone_rows == 2_000_000

    @pytest.mark.asyncio
    async def test_progress_update_with_zero_start_time(self):
        """Test progress update when start time is None."""
        progress = Progress(milestone_interval=1_000_000)
        # Don't call start() - run_start_time will be None

        with patch.object(progress.logger, "info") as mock_info:
            await progress.update(1_000_000)

            assert progress.total_rows_progress == 1_000_000
            # Should not log because elapsed time calculation would fail
            mock_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_progress_update_rate_calculation(self):
        """Test that progress update calculates rate correctly."""
        progress = Progress(milestone_interval=1_000_000)
        start_time = time.monotonic()
        progress.start(start_time)

        # Wait a bit to get non-zero elapsed time
        await asyncio.sleep(0.1)

        with patch.object(progress.logger, "info") as mock_info:
            await progress.update(1_000_000)

            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            # Verify rate is included in log message
            assert "rows/s" in call_args

    @pytest.mark.asyncio
    async def test_progress_update_exact_milestone_boundary(self):
        """Test progress update exactly at milestone boundary."""
        progress = Progress(milestone_interval=1_000_000)
        start_time = time.monotonic()
        progress.start(start_time)

        with patch.object(progress.logger, "info") as mock_info:
            # First update to exactly 1M
            await progress.update(1_000_000)
            assert mock_info.call_count == 1
            assert progress.last_milestone_rows == 1_000_000

            # Second update that doesn't cross next milestone
            await progress.update(500_000)
            # Should still be at 1M milestone (no new milestone logged)
            assert mock_info.call_count == 1
            assert progress.total_rows_progress == 1_500_000
