"""
Custom exceptions for hygge - clear, actionable error handling.

hygge uses a hierarchical exception system that makes error handling
comfortable and reliable. Exceptions are designed to be clear and actionable,
helping you understand what went wrong and how to fix it.

Following hygge's philosophy, exceptions prioritize:
- **Comfort**: Clear error messages that help you understand what happened
- **Reliability**: Proper exception chaining preserves context for debugging
- **Natural flow**: Transient errors are retried automatically,
  permanent errors fail fast

Exception Hierarchy:
    HyggeError (base)
    ├── FlowError
    │   ├── FlowExecutionError - General flow execution errors
    │   └── FlowConnectionError - Transient connection errors during flow execution
    ├── HomeError
    │   ├── HomeConnectionError - Connection errors when reading from home
    │   └── HomeReadError - Errors reading data from home
    ├── StoreError
    │   ├── StoreConnectionError - Connection errors when writing to store
    │   └── StoreWriteError - Errors writing data to store
    ├── JournalError
    │   └── JournalWriteError - Errors writing to journal
    └── ConfigError - Configuration errors

Usage Guidelines:
    - Use specific exceptions (HomeConnectionError, StoreWriteError, etc.) when you know
      the exact error type. This enables precise error handling and retry logic.
    - Always use exception chaining (`raise SpecificError(...) from e`) when wrapping
      exceptions to preserve the original traceback for debugging.
    - Connection errors (HomeConnectionError, StoreConnectionError, FlowConnectionError)
      are typically transient and should be retried.
    - Read/Write errors (HomeReadError, StoreWriteError) are typically non-transient and
      indicate data or operation issues that may not benefit from retries.
"""


class HyggeError(Exception):
    """Base exception for all hygge errors."""

    pass


class FlowError(HyggeError):
    """Base exception for flow-related errors."""

    pass


class FlowExecutionError(FlowError):
    """Error during flow execution."""

    pass


class FlowConnectionError(FlowError):
    """Transient connection error during flow execution."""

    pass


class HomeError(HyggeError):
    """Base exception for home-related errors."""

    pass


class HomeConnectionError(HomeError):
    """Connection error when reading from home."""

    pass


class HomeReadError(HomeError):
    """Error reading data from home."""

    pass


class StoreError(HyggeError):
    """Base exception for store-related errors."""

    def __init__(self, message: str, **kwargs):
        super().__init__(message)
        self.context = kwargs


class StoreConnectionError(StoreError):
    """Connection error when writing to store."""

    pass


class StoreWriteError(StoreError):
    """Error writing data to store."""

    pass


class ConfigError(HyggeError):
    """Raised when there's an error in configuration."""

    pass


class JournalError(HyggeError):
    """Base exception for journal-related errors."""

    pass


class JournalWriteError(JournalError):
    """Error writing to journal."""

    pass
