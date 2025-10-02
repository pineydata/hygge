"""
Logging configuration for hygge.
"""
import logging
import sys
from pathlib import Path
from typing import Optional

import colorama

# Initialize colorama for cross-platform color support
colorama.init()

class ColorFormatter(logging.Formatter):
    """Custom formatter with colors"""

    # Color codes
    COLORS = {
        "INFO": colorama.Fore.GREEN,
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
        "DEBUG": colorama.Fore.BLUE,
        "START": colorama.Fore.CYAN,
        "OK": colorama.Fore.GREEN,
    }

    def format(self, record):
        # Add color to levelname if it exists in our color mapping
        if record.levelname in self.COLORS:
            color = self.COLORS[record.levelname]
            record.levelname = (
                f"{color}{record.levelname}{colorama.Style.RESET_ALL}"
            )

        # Add color to message for START and OK prefixes
        if hasattr(record, "color_prefix"):
            color = self.COLORS.get(record.color_prefix, "")
            record.msg = f"{color}{record.msg}{colorama.Style.RESET_ALL}"

        return super().format(record)

class HyggeLogger:
    """Central logging class for hygge"""

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
            log_file = log_dir / "hygge.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                "%(asctime)s  %(message)s", datefmt="%H:%M:%S"
            )
            file_handler.setFormatter(file_formatter)

            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            color_formatter = ColorFormatter(
                "%(asctime)s  %(message)s", datefmt="%H:%M:%S"
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