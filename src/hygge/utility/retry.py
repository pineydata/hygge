"""
Retry decorator with exponential backoff and timeout for async functions.
"""
import asyncio
import logging
from functools import wraps
from typing import Callable, Optional, Tuple, Type, Union

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from hygge.messages import get_logger

from .exceptions import FlowError, HomeError, StoreError


def with_retry(
    timeout: int = 300,  # 5 minutes default
    retries: int = 3,
    delay: int = 2,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (
        HomeError,
        StoreError,
        FlowError,
    ),
    logger_name: str = "hygge.retry",
    retry_if_func: Optional[Callable] = None,
    before_sleep_func: Optional[Callable] = None,
):
    """
    Retry decorator with exponential backoff and timeout for async functions.

    Retries failed operations with exponential backoff between attempts.
    It also enforces a timeout for each attempt to prevent hanging operations.

    Args:
        timeout: Maximum time in seconds for each attempt (default: 300)
        retries: Maximum number of retry attempts (default: 3)
        delay: Initial delay between retries in seconds (default: 2)
        exceptions: Exception types to catch and retry on (default: hygge exceptions)
        logger_name: Name for logging retry attempts (default: hygge.retry)
        retry_if_func: Optional function to determine if error should be retried.
            Takes (exception) and returns bool. If provided, overrides exceptions.
        before_sleep_func: Optional async function called before each retry.
            Takes (retry_state) and can perform cleanup/setup.

    Example:
        @with_retry(retries=3, delay=2, exceptions=(StoreError,), timeout=60)
        async def write_batch(self, batch: DataFrame) -> str:
            # Function implementation

    Raises:
        TimeoutError: If an attempt exceeds the timeout period
        Any exception in exceptions tuple: If all retry attempts fail
    """
    logger = get_logger(logger_name)
    # Extract underlying standard logger for tenacity's before_sleep_log
    # HyggeLogger wraps a standard logger, so we need the actual logger object
    standard_logger = logger.logger if hasattr(logger, "logger") else logger

    def decorator(func):
        # Determine retry condition
        if retry_if_func:
            # Use retry_if_exception which retries only if predicate returns True
            # When predicate returns False, the exception is re-raised immediately
            # without retrying
            retry_condition = retry_if_exception(retry_if_func)
        else:
            retry_condition = retry_if_exception_type(exceptions)

        # Determine before_sleep callback
        if before_sleep_func:

            async def combined_before_sleep(retry_state):
                # Call custom function first
                await before_sleep_func(retry_state)
                # Then log
                before_sleep_log(standard_logger, logging.WARNING)(retry_state)

            before_sleep = combined_before_sleep
        else:
            before_sleep = before_sleep_log(standard_logger, logging.WARNING)

        @retry(
            stop=stop_after_attempt(retries),
            wait=wait_exponential(multiplier=delay, min=delay, max=delay * 8),
            retry=retry_condition,
            before_sleep=before_sleep,
        )
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                async with asyncio.timeout(timeout):
                    return await func(*args, **kwargs)
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"Operation {func.__name__} timed out after {timeout} seconds"
                )

        return wrapper

    return decorator
