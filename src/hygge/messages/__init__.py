"""
Message utilities for hygge.

This submodule provides messaging utilities for hygge:
- Logger: Human-readable output formatting with colors
- Progress: Progress tracking and milestone messages
- Summary: Hygge-style execution summary formatting
"""
from hygge.messages.logger import HyggeLogger, close_hygge_file_handlers, get_logger
from hygge.messages.progress import Progress
from hygge.messages.summary import Summary

__all__ = [
    "HyggeLogger",
    "close_hygge_file_handlers",
    "get_logger",
    "Progress",
    "Summary",
]
