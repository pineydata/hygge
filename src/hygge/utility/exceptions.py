"""
Custom exceptions for hygge.
"""

class HyggeError(Exception):
    """Base exception for all hygge errors."""
    pass

class FlowError(HyggeError):
    """Raised when there's an error in the flow."""
    pass

class HomeError(HyggeError):
    """Raised when there's an error reading from a data home."""
    pass

class StoreError(HyggeError):
    """Raised when there's an error writing to a data store."""

    def __init__(self, message: str, **kwargs):
        super().__init__(message)
        self.context = kwargs

class ConfigError(HyggeError):
    """Raised when there's an error in configuration."""
    pass
