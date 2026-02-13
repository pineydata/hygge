"""
Progress tracking for coordinator-level milestones - comfortable visibility.

Progress tracking makes it easy to see how your flows are progressing across
multiple flows. It provides milestone-based logging that feels natural and
informative without being overwhelming.
"""

import asyncio
from typing import Optional

from hygge.messages.logger import HyggeLogger, _get_event_loop_time


class Progress:
    """
    Tracks progress across multiple flows - comfortable visibility.

    Progress tracking makes it easy to see how your flows are progressing
    across multiple flows. It provides milestone-based logging (e.g., every
    1M rows) that feels natural and informative without being overwhelming.

    Following hygge's philosophy, progress tracking prioritizes:
    - **Comfort**: Clear milestones, not overwhelming detail
    - **Reliability**: Thread-safe updates, accurate row counts
    - **Natural flow**: Progress feels smooth and informative
    """

    def __init__(
        self, milestone_interval: int = 1_000_000, logger: Optional[HyggeLogger] = None
    ):
        """
        Initialize progress tracker.

        Args:
            milestone_interval: Number of rows between milestone logs (default: 1M)
            logger: Optional logger instance (default: creates new logger)
        """
        self.total_rows_progress = 0
        self.last_milestone_rows = 0
        self.milestone_interval = milestone_interval
        self.milestone_lock = asyncio.Lock()
        self.run_start_time: Optional[float] = None
        self.logger = logger or HyggeLogger("hygge.progress")

    def start(self, start_time: Optional[float] = None) -> None:
        """
        Start progress tracking.

        Args:
            start_time: Optional start time (default: current event loop time)
        """
        if start_time is None:
            start_time = _get_event_loop_time()
        self.run_start_time = start_time
        self.total_rows_progress = 0
        self.last_milestone_rows = 0

    async def update(self, rows: int) -> None:
        """
        Update progress and log milestones.

        Args:
            rows: Number of rows to add to progress
        """
        async with self.milestone_lock:
            self.total_rows_progress += rows
            current_total = self.total_rows_progress

            # Check if we've crossed any milestones since last log
            # Log at each 1M mark (1M, 2M, 3M, etc.)
            while current_total >= self.last_milestone_rows + self.milestone_interval:
                self.last_milestone_rows += self.milestone_interval
                milestone = self.last_milestone_rows

                elapsed = (
                    _get_event_loop_time() - self.run_start_time
                    if self.run_start_time
                    else 0.0
                )
                if elapsed > 0:
                    rate = milestone / elapsed
                    # Narrative milestone message
                    self.logger.info(
                        f"📊 Milestone: {milestone:,} rows moved in {elapsed:.1f}s "
                        f"({rate:,.0f} rows/s)"
                    )
