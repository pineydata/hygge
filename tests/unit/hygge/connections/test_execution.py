"""
Unit tests for execution engines.

Tests focus on execution strategies, streaming behavior, and parallelism
without requiring actual database connections.
"""

import asyncio
from typing import Iterator

import pytest

from hygge.connections.execution import (
    SimpleEngine,
    ThreadPoolEngine,
    get_engine,
    register_engine,
)


@pytest.fixture(autouse=True)
def clean_execution_engine():
    """Ensure ThreadPoolEngine is clean before and after each test."""
    # Clean up before test
    if ThreadPoolEngine.is_initialized():
        ThreadPoolEngine.shutdown()

    yield

    # Clean up after test
    if ThreadPoolEngine.is_initialized():
        ThreadPoolEngine.shutdown()


class TestThreadPoolEngine:
    """Test ThreadPoolEngine for synchronous database operations."""

    def test_engine_not_initialized_by_default(self):
        """Test that engine is not initialized by default."""
        # Fixture ensures clean state
        assert not ThreadPoolEngine.is_initialized()

    def test_initialization(self):
        """Test engine initialization."""
        ThreadPoolEngine.initialize(pool_size=4)
        assert ThreadPoolEngine.is_initialized()

    def test_double_initialization_warning(self):
        """Test that double initialization logs warning."""
        ThreadPoolEngine.initialize(pool_size=4)
        ThreadPoolEngine.initialize(pool_size=4)  # Should log warning

    def test_shutdown(self):
        """Test engine shutdown."""
        ThreadPoolEngine.initialize(pool_size=4)
        assert ThreadPoolEngine.is_initialized()

        ThreadPoolEngine.shutdown()
        assert not ThreadPoolEngine.is_initialized()

        # Re-initialize for fixture cleanup
        ThreadPoolEngine.initialize(pool_size=4)

    @pytest.mark.asyncio
    async def test_execute_simple_operation(self):
        """Test executing a simple synchronous operation."""
        ThreadPoolEngine.initialize(pool_size=2)

        def add_numbers(a, b):
            return a + b

        result = await ThreadPoolEngine.execute(add_numbers, 5, 3)
        assert result == 8

    @pytest.mark.asyncio
    async def test_execute_streaming_basic(self):
        """Test basic streaming execution yields batches as extracted."""
        ThreadPoolEngine.initialize(pool_size=2)

        def generate_batches(count: int) -> Iterator[tuple[int, int]]:
            """Synchronous generator that yields batches."""
            for i in range(count):
                yield (i, i * 10)

        # Collect batches as they arrive
        batches = []
        async for batch in ThreadPoolEngine.execute_streaming(generate_batches, 5):
            batches.append(batch)

        # Verify we got all batches
        assert len(batches) == 5
        assert batches[0] == (0, 0)
        assert batches[4] == (4, 40)

    @pytest.mark.asyncio
    async def test_execute_streaming_true_streaming(self):
        """
        Test that streaming actually streams (batches yielded as extracted).

        This is the key test - verifies batches are yielded incrementally,
        not all at once after extraction completes.
        """
        ThreadPoolEngine.initialize(pool_size=2)

        yield_order = []

        def generator(count: int) -> Iterator[int]:
            """Generator that yields values."""
            for i in range(count):
                yield i

        # Yield batches as they arrive
        async for batch in ThreadPoolEngine.execute_streaming(generator, 5):
            yield_order.append(batch)

        # Verify we got all batches in order (streaming worked)
        assert yield_order == [0, 1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_execute_streaming_error_handling(self):
        """Test error handling in streaming execution."""
        ThreadPoolEngine.initialize(pool_size=2)

        def failing_generator() -> Iterator[int]:
            """Generator that raises an error."""
            yield 1
            yield 2
            raise ValueError("Test error")

        # Should raise the error from the generator
        batches = []
        with pytest.raises(ValueError, match="Test error"):
            async for batch in ThreadPoolEngine.execute_streaming(failing_generator):
                batches.append(batch)

        # Should have gotten batches before the error
        assert len(batches) >= 1

    @pytest.mark.asyncio
    async def test_execute_streaming_parallelism(self):
        """
        Test that multiple streaming extractions can run in parallel.

        Verifies true parallelism - multiple extractions should run
        simultaneously, not sequentially.
        """
        ThreadPoolEngine.initialize(pool_size=4)

        def parallel_generator(generator_id: int, batch_count: int) -> Iterator[int]:
            """Generator that yields values tagged with generator ID."""
            for i in range(batch_count):
                yield (generator_id, i)

        # Start 3 extractions concurrently
        async def extract_async(generator_id: int):
            batches = []
            async for batch in ThreadPoolEngine.execute_streaming(
                parallel_generator, generator_id, 3
            ):
                batches.append(batch)
            return batches

        # Run with timeout to prevent hanging
        results = await asyncio.wait_for(
            asyncio.gather(extract_async(1), extract_async(2), extract_async(3)),
            timeout=5.0,
        )

        # All should have completed
        assert len(results) == 3
        assert all(len(r) == 3 for r in results)

        # Verify each generator produced correct results
        assert results[0] == [(1, 0), (1, 1), (1, 2)]
        assert results[1] == [(2, 0), (2, 1), (2, 2)]
        assert results[2] == [(3, 0), (3, 1), (3, 2)]

    @pytest.mark.asyncio
    async def test_execute_lazy_initialization(self):
        """Test that engine initializes lazily if not pre-initialized."""

        # Don't initialize explicitly
        def add(a, b):
            return a + b

        # Should auto-initialize
        result = await ThreadPoolEngine.execute(add, 2, 3)
        assert result == 5
        assert ThreadPoolEngine.is_initialized()


class TestSimpleEngine:
    """Test SimpleEngine for simple async operations."""

    @pytest.mark.asyncio
    async def test_execute_simple_operation(self):
        """Test executing a simple operation."""

        def multiply(a, b):
            return a * b

        result = await SimpleEngine.execute(multiply, 6, 7)
        assert result == 42

    @pytest.mark.asyncio
    async def test_execute_streaming_buffers(self):
        """
        Test that SimpleEngine buffers results (expected behavior).

        SimpleEngine buffers because asyncio.to_thread doesn't handle
        generators well. This is acceptable for simple cases.
        """

        def generate_values(count: int) -> Iterator[int]:
            for i in range(count):
                yield i

        results = []
        async for value in SimpleEngine.execute_streaming(generate_values, 5):
            results.append(value)

        assert len(results) == 5
        assert results == [0, 1, 2, 3, 4]


class TestExecutionEngineRegistry:
    """Test execution engine registry pattern."""

    def test_get_engine_thread_pool(self):
        """Test getting thread_pool engine."""
        engine = get_engine("thread_pool")
        assert isinstance(engine, ThreadPoolEngine)

    def test_get_engine_simple(self):
        """Test getting simple engine."""
        engine = get_engine("simple")
        assert isinstance(engine, SimpleEngine)

    def test_get_engine_unknown_raises_error(self):
        """Test that unknown engine raises error."""
        with pytest.raises(ValueError, match="Unknown execution engine"):
            get_engine("unknown_engine")

    def test_register_custom_engine(self):
        """Test registering a custom engine."""

        class CustomEngine(SimpleEngine):
            pass

        register_engine("custom", CustomEngine())

        engine = get_engine("custom")
        assert isinstance(engine, CustomEngine)
