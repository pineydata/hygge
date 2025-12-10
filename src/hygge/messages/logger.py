"""
Logging configuration for hygge - comfortable, informative output.

HyggeLogger provides human-readable, color-coded logging that makes it easy
to see what's happening in your flows. It follows hygge's philosophy of comfort
and clarity, providing informative output without overwhelming you with details.
"""
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import colorama

# Initialize colorama for cross-platform color support
colorama.init()


def _get_event_loop_time() -> float:
    """
    Get current event loop time, handling both async and sync contexts.

    Tries to get the running loop first (Python 3.7+), falls back to
    getting the event loop if no running loop exists. If no event loop
    is available at all (e.g., in sync contexts or tests), falls back
    to time.monotonic() which provides similar monotonic time behavior.

    Returns:
        Current event loop time in seconds (or monotonic time if no loop)
    """
    try:
        # Try to get running loop first (preferred in Python 3.7+)
        loop = asyncio.get_running_loop()
        return loop.time()
    except RuntimeError:
        # No running loop - try to get event loop
        try:
            loop = asyncio.get_event_loop()
            return loop.time()
        except RuntimeError:
            # No event loop available - fall back to monotonic time
            # This works in sync contexts and tests
            return time.monotonic()


class ColorFormatter(logging.Formatter):
    """Custom formatter with colors"""

    # Color codes - Blue and Green scheme
    COLORS = {
        "INFO": colorama.Fore.BLUE,
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
        "DEBUG": colorama.Fore.BLUE,
        "START": colorama.Fore.BLUE,
        "OK": colorama.Fore.GREEN,
    }

    def format(self, record):
        # Extract flow name from logger name
        # (e.g., hygge.flow.dividends_lots -> dividends_lots)
        # and format it in white
        if record.name.startswith("hygge.flow."):
            flow_name = record.name.replace("hygge.flow.", "")
            # Remove .home or .store suffix if present
            if ".home" in flow_name:
                flow_name = flow_name.replace(".home", "")
            elif ".store" in flow_name:
                flow_name = flow_name.replace(".store", "")

            # Add white-colored flow name to record with trailing space
            white = colorama.Fore.WHITE
            reset = colorama.Style.RESET_ALL
            record.flow_name = f"{white}[{flow_name}]{reset} "
        else:
            record.flow_name = ""

        # Add color to levelname if it exists in our color mapping
        if record.levelname in self.COLORS:
            color = self.COLORS[record.levelname]
            record.levelname = f"{color}{record.levelname}{colorama.Style.RESET_ALL}"

        # Add color to message for START and OK prefixes
        if hasattr(record, "color_prefix"):
            color = self.COLORS.get(record.color_prefix, "")
            record.msg = f"{color}{record.msg}{colorama.Style.RESET_ALL}"

        return super().format(record)


class HyggeLogger:
    """
    Central logging class for hygge - comfortable, informative output.

    HyggeLogger provides human-readable, color-coded logging that makes it easy
    to see what's happening in your flows. It follows hygge's philosophy of
    comfort and clarity, providing informative output without overwhelming you
    with details.

    The logger automatically formats flow names, uses colors for different log
    levels, and writes to both console and log files for easy debugging.
    """

    class Style:
        """ANSI color codes for paths"""

        CYAN = colorama.Fore.CYAN
        GREEN = colorama.Fore.GREEN
        YELLOW = colorama.Fore.YELLOW
        BLUE = colorama.Fore.BLUE
        MAGENTA = colorama.Fore.MAGENTA
        RED = colorama.Fore.RED
        RESET = colorama.Style.RESET_ALL

    # Logging templates for consistent formatting
    EXTRACT_TEMPLATE = "Extracted {:,} rows in {:.1f}s ({:,.0f} rows/s)"
    LOAD_TEMPLATE = "Loaded {:,} rows in {:.1f}s ({:,.0f} rows/s)"
    WRITE_TEMPLATE = "Processed {:,} rows in {:.1f}s ({:,.0f} rows/s)"
    BATCH_TEMPLATE = "Processed {:,} rows in {:.1f}s ({:,.0f} rows/s)"

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.style = self.Style()

        # Only set up handlers if they haven't been set up already
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)

            # Get project root directory
            project_root = Path.cwd()

            # Create logs directory if needed
            log_dir = project_root / "logs"
            log_dir.mkdir(exist_ok=True)

            # File handler
            # (also use ColorFormatter for flow name extraction)
            log_file = log_dir / "hygge.log"
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)
            file_formatter = ColorFormatter(
                "%(asctime)s  %(flow_name)s%(message)s", datefmt="%H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)

            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            color_formatter = ColorFormatter(
                "%(asctime)s  %(flow_name)s%(message)s", datefmt="%H:%M:%S"
            )
            console_handler.setFormatter(color_formatter)

            # Add handlers
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

            # Prevent logs from being passed to root logger
            self.logger.propagate = False

    def info(self, msg: str, color_prefix: Optional[str] = None) -> None:
        """Log info message with optional color prefix"""
        extra = {"color_prefix": color_prefix} if color_prefix else None
        self.logger.info(msg, extra=extra)

    def status(self, message: str) -> None:
        """Log a status message"""
        self.logger.info(f"STATUS: {message}")

    def start(self, msg: str) -> None:
        """Log start message in cyan"""
        self.info(f"START {msg}", color_prefix="START")

    def success(self, msg: str) -> None:
        """Log success message in green"""
        self.info(f"OK {msg}", color_prefix="OK")

    def error(self, msg: str) -> None:
        """Log error message in red"""
        self.logger.error(msg)

    def warning(self, msg: str) -> None:
        """Log warning message in yellow"""
        self.logger.warning(msg)

    def debug(self, msg: str) -> None:
        """Log debug message in blue"""
        self.logger.debug(msg)

    def path(self, path: str, color: str = None) -> str:
        """Format a path with color"""
        if not color:
            color = self.style.CYAN
        return f"{color}{path}{self.style.RESET}"


def get_logger(name: str) -> HyggeLogger:
    """Get a configured logger instance."""
    return HyggeLogger(name)
