"""
Hygge-style execution summaries - comfortable, informative summaries.

Summaries that feel cozy and helpful, not just informative. They reflect
hygge's values of comfort, clarity, and natural flow, making it easy to
understand what happened in your flows at a glance.
"""
from typing import Any, Dict, List, Optional

from hygge.messages.logger import HyggeLogger, _get_event_loop_time


class Summary:
    """
    Generates hygge-style execution summaries - comfortable and informative.

    Summary provides cozy, helpful summaries that make it easy to understand
    what happened in your flows. It reflects hygge's values of comfort,
    clarity, and natural flow, showing you the important information without
    overwhelming you with details.
    """

    def __init__(self, logger: Optional[HyggeLogger] = None):
        """
        Initialize summary generator.

        Args:
            logger: Optional logger instance (default: creates new logger)
        """
        self.logger = logger or HyggeLogger("hygge.summary")

    def generate_summary(
        self,
        flow_results: List[Dict[str, Any]],
        start_time: Optional[float] = None,
    ) -> None:
        """
        Generate and log execution summary.

        Args:
            flow_results: List of flow result dictionaries with keys:
                - name: Flow name
                - status: "pass", "fail", or "skip"
                - rows: Number of rows processed
                - duration: Duration in seconds
                - error: Optional error message (for failures)
            start_time: Optional start time for calculating elapsed time
                        (default: uses current event loop time, which may be incorrect)
        """
        if not flow_results:
            return

        elapsed_time = (
            _get_event_loop_time() - start_time if start_time is not None else 0.0
        )

        total_rows = sum(r["rows"] for r in flow_results)
        passed = sum(1 for r in flow_results if r["status"] == "pass")
        failed = sum(1 for r in flow_results if r["status"] == "fail")
        skipped = sum(1 for r in flow_results if r["status"] == "skip")

        # Hygge-style summary - clean, cozy, and information-dense
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = elapsed_time % 60

        # Build time string conditionally based on non-zero units
        time_parts = []
        if hours > 0:
            time_parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            time_parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        # Always include seconds
        time_parts.append(f"{seconds:.2f} second{'s' if seconds != 1.0 else ''}")

        if len(time_parts) > 1:
            time_str = ", ".join(time_parts[:-1]) + f" and {time_parts[-1]}"
        else:
            time_str = time_parts[0]

        # Add cozy spacing
        self.logger.info("")

        # Hygge-style summary line - comfortable and clear
        flow_word = "flow" if len(flow_results) == 1 else "flows"
        self.logger.info(
            f"Finished running {len(flow_results)} {flow_word} "
            f"in {time_str} ({elapsed_time:.2f}s)."
        )

        # Final status line (green if all pass, red if failures)
        if failed == 0:
            self.logger.info("Completed successfully", color_prefix="OK")
        else:
            self.logger.error("Completed with errors")

        # Hygge-style status summary - natural and concise
        if failed == 0 and skipped == 0:
            # All passed - simple case
            self.logger.info(f"{passed} {'flow' if passed == 1 else 'flows'} passed.")
        else:
            # Mixed results - show what matters
            parts = []
            if passed > 0:
                parts.append(f"{passed} passed")
            if failed > 0:
                parts.append(f"{failed} failed")
            if skipped > 0:
                parts.append(f"{skipped} skipped")

            status_str = ", ".join(parts)
            self.logger.info(f"{status_str} ({len(flow_results)} total).")

        # Optional: Show total rows processed
        if total_rows > 0:
            self.logger.info(f"Total rows processed: {total_rows:,}")
            if elapsed_time > 0:
                rate = total_rows / elapsed_time
                self.logger.info(f"Overall rate: {rate:,.0f} rows/s")

        # Add cozy spacing at end
        self.logger.info("")

        # Show failed flow details - helpful, not just informative
        if failed > 0:
            self.logger.error("Failed flows:")
            for flow_result in flow_results:
                if flow_result["status"] == "fail":
                    error_msg = flow_result.get("error", "Unknown error")
                    self.logger.error(f"  {flow_result['name']}: {error_msg}")
