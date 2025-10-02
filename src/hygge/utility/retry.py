"""
Retry decorator with exponential backoff and timeout for async functions.
"""
import asyncio
import logging
from functools import wraps
from typing import Type, Tuple, Union

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .exceptions import FlowError, HomeError, StoreError
from .logger import get_logger


def with_retry(
    timeout: int = 300,  # 5 minutes default
    retries: int = 3,
    delay: int = 2,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (
        HomeError, StoreError, FlowError
    ),
    logger_name: str = "hygge.retry",
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

    Example:
        @with_retry(retries=3, delay=2, exceptions=(StoreError,), timeout=60)
        async def write_batch(self, batch: DataFrame) -> str:
            # Function implementation

    Raises:
        TimeoutError: If an attempt exceeds the timeout period
        Any exception in exceptions tuple: If all retry attempts fail
    """
    logger = get_logger(logger_name)

    def decorator(func):
        @retry(
            stop=stop_after_attempt(retries),
            wait=wait_exponential(multiplier=delay, min=delay, max=delay * 8),
            retry=retry_if_exception_type(exceptions),
            before_sleep=before_sleep_log(logger, logging.WARNING)
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
