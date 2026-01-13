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

        # Celebratory completion or compassionate failure
        if failed == 0:
            self._generate_success_summary(
                flow_results, passed, skipped, total_rows, elapsed_time, time_str
            )
        else:
            self._generate_error_summary(
                flow_results,
                passed,
                failed,
                skipped,
                total_rows,
                elapsed_time,
                time_str,
            )

        # Add cozy spacing at end
        self.logger.info("")

    def _generate_success_summary(
        self,
        flow_results: List[Dict[str, Any]],
        passed: int,
        skipped: int,
        total_rows: int,
        elapsed_time: float,
        time_str: str,
    ) -> None:
        """Generate warm, celebratory success summary."""
        self._log_success_header(passed, skipped)
        self._log_success_stats(passed, skipped, total_rows, elapsed_time, time_str)
        self._log_settled_flows(flow_results)

    def _log_success_header(self, passed: int, skipped: int) -> None:
        """Log celebratory header."""
        if passed == 1 and skipped == 0:
            self.logger.info("✨ All done! Your data is home.", color_prefix="OK")
        else:
            self.logger.info(
                "✨ All done! Your data is cozy and settled.", color_prefix="OK"
            )

    def _log_success_stats(
        self,
        passed: int,
        skipped: int,
        total_rows: int,
        elapsed_time: float,
        time_str: str,
    ) -> None:
        """Log success statistics."""
        self.logger.info("")
        self.logger.info("📊 Summary:")

        # Flow completion status
        flow_word = "flow" if passed == 1 else "flows"
        if skipped == 0:
            self.logger.info(f"   • {passed} {flow_word} completed successfully")
        else:
            parts = []
            if passed > 0:
                parts.append(f"{passed} completed")
            if skipped > 0:
                parts.append(f"{skipped} skipped")
            self.logger.info(f"   • {', '.join(parts)} ({passed + skipped} total)")

        # Row count and timing
        if total_rows > 0:
            self.logger.info(f"   • {total_rows:,} rows moved to their new home")

        if elapsed_time > 0:
            rate = total_rows / elapsed_time if total_rows > 0 else 0
            timing = f"   • Finished in {time_str}"
            if rate > 0:
                timing += f" ({rate:,.0f} rows/s)"
            self.logger.info(timing)

    def _log_settled_flows(self, flow_results: List[Dict[str, Any]]) -> None:
        """Log where data settled for successful flows."""
        passed_flows = [r for r in flow_results if r["status"] == "pass"]
        if not passed_flows or len(passed_flows) > 10:
            return  # Skip if no flows or too many to show

        self.logger.info("")
        self.logger.info("🏡 Your data is settled:")
        for flow_result in passed_flows:
            rows = flow_result.get("rows", 0)
            rows_info = f"({rows:,} rows)" if rows > 0 else ""
            self.logger.info(f"   ✓ {flow_result['name']} {rows_info}")

    def _generate_error_summary(
        self,
        flow_results: List[Dict[str, Any]],
        passed: int,
        failed: int,
        skipped: int,
        total_rows: int,
        elapsed_time: float,
        time_str: str,
    ) -> None:
        """Generate compassionate error summary with guidance."""
        self.logger.error("⚠️  Some flows need attention")
        self._log_error_stats(passed, failed, skipped, total_rows, time_str)
        self._log_failed_flows(flow_results)
        self._log_next_steps()

    def _log_error_stats(
        self, passed: int, failed: int, skipped: int, total_rows: int, time_str: str
    ) -> None:
        """Log error summary statistics."""
        self.logger.info("")
        self.logger.info("📊 Summary:")

        # Build status summary
        parts = []
        if passed > 0:
            parts.append(f"{passed} succeeded")
        if failed > 0:
            parts.append(f"{failed} failed")
        if skipped > 0:
            parts.append(f"{skipped} skipped")

        total = passed + failed + skipped
        self.logger.info(f"   • {', '.join(parts)} ({total} total)")

        if total_rows > 0:
            self.logger.info(f"   • {total_rows:,} rows processed")

        self.logger.info(f"   • Ran for {time_str}")

    def _log_failed_flows(self, flow_results: List[Dict[str, Any]]) -> None:
        """Log details about failed flows."""
        self.logger.info("")
        self.logger.error("❌ Flows that need attention:")
        for flow_result in flow_results:
            if flow_result["status"] == "fail":
                error_msg = flow_result.get("error", "Unknown error")
                first_line = error_msg.split("\n")[0]  # Just first line for summary
                self.logger.error(f"   • {flow_result['name']}")
                self.logger.error(f"     {first_line}")

    def _log_next_steps(self) -> None:
        """Log helpful next steps for fixing errors."""
        self.logger.info("")
        self.logger.info("💡 Next steps:")
        self.logger.info("   • Check the error messages above")
        self.logger.info("   • Run: hygge debug --flow <flow_name>")
        self.logger.info("   • Fix the issues and try again")
