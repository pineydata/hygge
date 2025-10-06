"""
Updated tests for the Home base class.

Following hygge's testing principles:
- Test behavior that makes sense to users
- Focus on data reading and batch processing
- Keep tests clear and maintainable
- Test the actual Home API as implemented
"""
import asyncio
from typing import AsyncIterator

import polars as pl
import pytest

from hygge.core.home import Home


class SimpleHome(Home):
    """Test implementation of Home for verification."""

    def __init__(self, name: str, data: list[pl.DataFrame], **kwargs):
        super().__init__(name, kwargs)
        self.data = data
        self.read_called = False
        self.close_called = False

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Simple implementation that yields test data."""
        self.read_called = True
        for df in self.data:
            yield df
            # Small delay to simulate real data reading
            await asyncio.sleep(0.01)

    def get_data_path(self):
        return f"/test/home/{self.name}"


class ErrorHome(Home):
    """Test implementation that raises errors."""

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Implementation that raises error before any data."""
        raise ValueError("Test error")
        # Unreachable yield to make this an async generator
        if False:
            yield pl.DataFrame()

    def get_data_path(self):
        return f"/test/home/{self.name}"


class DelayedHome(Home):
    """Test implementation with delays."""

    def __init__(
        self, name: str, data: list[pl.DataFrame], delay: float = 0.1, **kwargs
    ):
        super().__init__(name, kwargs)
        self.data = data
        self.delay = delay

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Implementation with configurable delays."""
        for df in self.data:
            yield df
            await asyncio.sleep(self.delay)

    def get_data_path(self):
        return f"/test/home/{self.name}"


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return [
        pl.DataFrame({"id": range(100), "value": ["test"] * 100}),
        pl.DataFrame({"id": range(100, 200), "value": ["test2"] * 100}),
        pl.DataFrame({"id": range(200, 250), "value": ["test3"] * 50}),
    ]


@pytest.fixture
def empty_data():
    """Create empty data for testing."""
    return []


@pytest.fixture
def simple_home(sample_data):
    """Create a simple home instance."""
    return SimpleHome("test_home", sample_data, batch_size=100)


@pytest.fixture
def error_home():
    """Create an error home instance."""
    return ErrorHome("error_home")


@pytest.fixture
def delayed_home():
    """Create a delayed home instance."""
    data = [pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})]
    return DelayedHome("delayed_home", data, delay=0.1)


class TestHomeInitialization:
    """Test Home initialization and configuration."""

    def test_home_initialization_defaults(self):
        """Test Home initializes with correct defaults."""
        home = SimpleHome("test", [])

        assert home.name == "test"
        assert home.batch_size == 10_000  # Default batch size
        assert home.row_multiplier == 300_000  # Default row multiplier
        assert home.options == {}
        assert home.start_time is None

    def test_home_initialization_custom_options(self):
        """Test Home respects custom configuration options."""
        options = {"batch_size": 5000, "row_multiplier": 100000}
        home = SimpleHome("test", [], **options)

        assert home.name == "test"
        assert home.batch_size == 5000
        assert home.row_multiplier == 100000
        assert home.options == options

    @pytest.mark.asyncio
    async def test_home_initialization_missing_implementation(self):
        """Test that incomplete Home implementations fail appropriately."""

        class IncompleteHome(Home):
            pass  # Missing all required methods

        home = IncompleteHome("incomplete", {})

        # read() should raise NotImplementedError when _get_batches not implemented
        with pytest.raises(NotImplementedError):
            async for batch in home.read():
                pass


class TestHomeDataReading:
    """Test Home data reading functionality."""

    @pytest.mark.asyncio
    async def test_home_reads_data_correctly(self, simple_home, sample_data):
        """Test Home reads data correctly."""
        # When reading data
        batches = []
        async for batch in simple_home.read():
            batches.append(batch)

        # Then should have read all data
        assert simple_home.read_called
        assert len(batches) == len(sample_data)

        # And data should match
        for i, batch in enumerate(batches):
            assert batch.equals(sample_data[i])

    @pytest.mark.asyncio
    async def test_home_handles_empty_data(self, empty_data):
        """Test Home handles empty data gracefully."""
        empty_home = SimpleHome("empty", empty_data)

        # When reading empty data
        batches = []
        async for batch in empty_home.read():
            batches.append(batch)

        # Then should complete successfully
        assert empty_home.read_called
        assert len(batches) == 0

    @pytest.mark.asyncio
    async def test_home_progress_tracking(self, simple_home):
        """Test Home tracks progress correctly."""
        # When reading data
        total_rows = 0
        async for batch in simple_home.read():
            total_rows += len(batch)

        # Then should track timing
        assert simple_home.start_time is not None
        elapsed = asyncio.get_event_loop().time() - simple_home.start_time
        assert elapsed > 0  # Should take some time due to async delays

    @pytest.mark.asyncio
    async def test_home_handles_errors(self, error_home):
        """Test Home handles errors gracefully."""
        # When reading data that causes error
        with pytest.raises(ValueError) as exc_info:
            async for batch in error_home.read():
                pass  # Should not reach this

        # Then should raise ValueError with proper message
        assert "Test error" in str(exc_info.value)


class TestHomeLifecycle:
    """Test Home lifecycle management."""

    @pytest.mark.asyncio
    async def test_home_lifecycle_management(self, simple_home):
        """Test Home lifecycle management."""
        # When using the home normally
        batches = []
        async for batch in simple_home.read():
            batches.append(batch)

        # Then should complete without error
        # (lifecycle is handled automatically)
        assert len(batches) > 0

    @pytest.mark.asyncio
    async def test_home_batch_size_behavior(self):
        """Test Home respects batch_size in _get_batches."""
        # Create data that's larger than batch_size
        large_data = pl.DataFrame({"id": range(15000), "value": ["test"] * 15000})

        class BatchAwareHome(Home):
            def __init__(self, data, **kwargs):
                super().__init__("batch_test", **kwargs)
                self.data = data

            async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
                # Simulate reading in batches
                for i in range(0, len(self.data), self.batch_size):
                    batch = self.data.slice(i, self.batch_size)
                    yield batch
                    await asyncio.sleep(0.001)

            def get_data_path(self):
                return "/test/path"

        batch_home = BatchAwareHome(large_data, options={"batch_size": 5000})

        # When reading data
        batch_count = 0
        async for batch in batch_home.read():
            batch_count += 1
            assert len(batch) <= batch_home.batch_size

        # Then should have multiple batches
        assert batch_count == 3  # 15000 / 5000 = 3 batches


class TestHomeConcurrency:
    """Test Home behavior in concurrent scenarios."""

    @pytest.mark.asyncio
    async def test_home_concurrent_access(self, delayed_home):
        """Test Home behavior with concurrent access."""

        # Create multiple readers
        async def reader(reader_id):
            batches = []
            async for batch in delayed_home.read():
                batches.append(batch)
            return reader_id, len(batches)

        # Read concurrently
        results = await asyncio.gather(*[reader(i) for i in range(3)])

        # Each should get the same data
        for reader_id, batch_count in results:
            assert batch_count == 1  # One batch for each reader

    @pytest.mark.asyncio
    async def test_home_cancellation_handling(self, delayed_home):
        """Test Home handles cancellation gracefully."""

        # Start reading with delay
        async def slow_read():
            async for batch in delayed_home.read():
                await asyncio.sleep(0.1)  # Longer delay

        # Start task and cancel it
        task = asyncio.create_task(slow_read())
        await asyncio.sleep(0.05)  # Let it start
        task.cancel()

        # Should handle cancellation gracefully
        with pytest.raises(asyncio.CancelledError):
            await task
