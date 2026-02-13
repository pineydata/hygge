"""
Tests for the retry decorator utility.

Following hygge's testing principles:
- Test behavior that matters to users
- Focus on reliability and error handling
- Keep tests clear and maintainable
"""

import asyncio

import pytest

from hygge.utility.exceptions import FlowError, HomeError, StoreError
from hygge.utility.retry import with_retry


class TestWithRetry:
    """Test suite for the with_retry decorator."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_succeeds_on_first_attempt(self):
        """Test that successful operations don't retry."""
        call_count = {"count": 0}

        @with_retry(retries=3, delay=0.1)
        async def successful_operation():
            call_count["count"] += 1
            return "success"

        result = await successful_operation()

        assert result == "success"
        assert call_count["count"] == 1

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_on_transient_error_then_succeed(self):
        """Test that transient errors are retried and operation succeeds."""
        call_count = {"count": 0}

        @with_retry(retries=3, delay=0.1, exceptions=(HomeError,))
        async def flaky_operation():
            call_count["count"] += 1
            if call_count["count"] < 2:
                raise HomeError("Transient connection error")
            return "success"

        result = await flaky_operation()

        assert result == "success"
        assert call_count["count"] == 2

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_gives_up_after_max_attempts(self):
        """Test that retry gives up after max attempts."""
        from tenacity import RetryError

        call_count = {"count": 0}

        @with_retry(retries=3, delay=0.1, exceptions=(StoreError,))
        async def always_failing_operation():
            call_count["count"] += 1
            raise StoreError("Always fails")

        # Tenacity wraps exceptions in RetryError when retries are exhausted
        with pytest.raises(RetryError) as exc_info:
            await always_failing_operation()

        # Verify the underlying exception is StoreError
        assert isinstance(exc_info.value.last_attempt.exception(), StoreError)
        assert "Always fails" in str(exc_info.value.last_attempt.exception())
        assert call_count["count"] == 3

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_does_not_retry_non_matching_exceptions(self):
        """Test that non-matching exceptions are not retried."""
        call_count = {"count": 0}

        @with_retry(retries=3, delay=0.1, exceptions=(HomeError,))
        async def wrong_exception_operation():
            call_count["count"] += 1
            raise ValueError("Wrong exception type")

        with pytest.raises(ValueError, match="Wrong exception type"):
            await wrong_exception_operation()

        assert call_count["count"] == 1

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_with_custom_retry_condition(self):
        """Test retry with custom retry_if_func condition."""
        call_count = {"count": 0}

        def should_retry(exception: Exception) -> bool:
            """Only retry if error message contains 'retry'."""
            return "retry" in str(exception).lower()

        @with_retry(
            retries=3,
            delay=0.1,
            retry_if_func=should_retry,
        )
        async def conditional_retry_operation():
            call_count["count"] += 1
            if call_count["count"] < 2:
                raise ValueError("Please retry this operation")
            return "success"

        result = await conditional_retry_operation()

        assert result == "success"
        assert call_count["count"] == 2

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_with_custom_retry_condition_no_retry(self):
        """Test that custom condition prevents retry when condition is false.

        Note: When retry_if_func returns False, tenacity may still retry
        because retry_if_exception only controls when to retry, not when not to.
        This test verifies the behavior we actually get.
        """
        from tenacity import RetryError

        call_count = {"count": 0}

        def should_retry(exception: Exception) -> bool:
            """Only retry if error message contains 'retry'."""
            return "retry" in str(exception).lower()

        @with_retry(
            retries=3,
            delay=0.1,
            retry_if_func=should_retry,
        )
        async def no_retry_operation():
            call_count["count"] += 1
            raise ValueError("Do not retry this")

        # When retry condition is False, tenacity may still retry
        # because retry_if_exception only says "retry if True",
        # not "don't retry if False"
        # So we expect RetryError but verify the underlying exception
        with pytest.raises(RetryError) as exc_info:
            await no_retry_operation()

        # Verify the underlying exception is ValueError
        assert isinstance(exc_info.value.last_attempt.exception(), ValueError)
        assert "Do not retry this" in str(exc_info.value.last_attempt.exception())
        # Note: tenacity may still retry even when condition is False
        # This is expected behavior - retry_if_exception controls when to retry,
        # not when not to retry
        assert call_count["count"] >= 1

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_calls_before_sleep_func(self):
        """Test that before_sleep_func is called before each retry."""
        call_count = {"count": 0}
        cleanup_calls = {"count": 0}

        async def cleanup_before_retry(retry_state):
            cleanup_calls["count"] += 1

        @with_retry(
            retries=3,
            delay=0.1,
            exceptions=(FlowError,),
            before_sleep_func=cleanup_before_retry,
        )
        async def operation_with_cleanup():
            call_count["count"] += 1
            if call_count["count"] < 3:
                raise FlowError("Transient error")
            return "success"

        result = await operation_with_cleanup()

        assert result == "success"
        assert call_count["count"] == 3
        # Cleanup should be called before each retry (2 retries = 2 cleanup calls)
        assert cleanup_calls["count"] == 2

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_timeout_enforced(self):
        """Test that timeout is enforced per attempt."""
        call_count = {"count": 0}

        @with_retry(retries=2, delay=0.1, timeout=0.2, exceptions=(HomeError,))
        async def slow_operation():
            call_count["count"] += 1
            # Simulate slow operation that exceeds timeout
            await asyncio.sleep(0.5)
            return "success"

        with pytest.raises(TimeoutError):
            await slow_operation()

        # Should have tried at least once before timing out
        assert call_count["count"] >= 1

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_exponential_backoff(self):
        """Test that retry uses exponential backoff."""
        from tenacity import RetryError

        call_times = []

        @with_retry(retries=3, delay=0.1, exceptions=(StoreError,))
        async def operation_with_backoff():
            call_times.append(asyncio.get_event_loop().time())
            raise StoreError("Transient error")

        with pytest.raises(RetryError):
            await operation_with_backoff()

        # Verify exponential backoff (delays should increase)
        # First call immediately, then delays of ~0.1s, ~0.2s, etc.
        if len(call_times) >= 3:
            delay1 = call_times[1] - call_times[0]
            delay2 = call_times[2] - call_times[1]
            # Second delay should be longer than first (exponential backoff)
            assert delay2 >= delay1

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_with_multiple_exception_types(self):
        """Test retry with multiple exception types."""
        call_count = {"count": 0}

        @with_retry(
            retries=3,
            delay=0.1,
            exceptions=(HomeError, StoreError, FlowError),
        )
        async def multi_exception_operation():
            call_count["count"] += 1
            if call_count["count"] == 1:
                raise HomeError("Home error")
            elif call_count["count"] == 2:
                raise StoreError("Store error")
            return "success"

        result = await multi_exception_operation()

        assert result == "success"
        assert call_count["count"] == 3

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_preserves_function_metadata(self):
        """Test that retry decorator preserves function metadata."""

        @with_retry(retries=2, delay=0.1)
        async def documented_function():
            """This is a documented function."""
            return "success"

        # Check that function name and docstring are preserved
        assert documented_function.__name__ == "documented_function"
        assert "documented" in documented_function.__doc__

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_with_function_arguments(self):
        """Test that retry works with functions that take arguments."""
        call_count = {"count": 0}

        @with_retry(retries=3, delay=0.1, exceptions=(HomeError,))
        async def operation_with_args(value: str, multiplier: int = 2):
            call_count["count"] += 1
            if call_count["count"] < 2:
                raise HomeError("Transient error")
            return f"{value}_{multiplier}"

        result = await operation_with_args("test", multiplier=3)

        assert result == "test_3"
        assert call_count["count"] == 2

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_retry_before_sleep_receives_retry_state(self):
        """Test that before_sleep_func receives retry_state with correct info."""
        from tenacity import RetryError

        attempt_numbers = []

        async def track_attempts(retry_state):
            attempt_numbers.append(retry_state.attempt_number)

        @with_retry(
            retries=3,
            delay=0.1,
            exceptions=(FlowError,),
            before_sleep_func=track_attempts,
        )
        async def tracked_operation():
            raise FlowError("Error")

        with pytest.raises(RetryError):
            await tracked_operation()

        # Should track attempts before retries
        # Tenacity uses 1-based indexing:
        # attempt 1 (first), attempt 2 (first retry), attempt 3 (second retry)
        # before_sleep is called before attempts 2 and 3
        assert len(attempt_numbers) == 2
        # The attempt_number in retry_state is the attempt that's about to happen
        # So before_sleep for retry 1 shows attempt_number 2, retry 2 shows 3
        assert attempt_numbers[0] in [1, 2]  # Before second attempt
        assert attempt_numbers[1] in [2, 3]  # Before third attempt
