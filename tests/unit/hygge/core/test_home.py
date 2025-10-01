"""
Tests for the Home base class.
"""
import pytest
import asyncio
import polars as pl
from datetime import datetime
from typing import AsyncIterator
from hygge import Home
from hygge.utils.exceptions import HomeError

class MockHome(Home):
    """Mock implementation of Home for testing."""
    def __init__(self, name: str, data: list[dict], **kwargs):
        super().__init__(name, kwargs)
        self.data = data
        self.iterator_called = False

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Mock implementation that returns predefined data in batches."""
        self.iterator_called = True
        # Split data into batches based on batch_size
        for i in range(0, len(self.data), self.batch_size):
            batch = self.data[i:i + self.batch_size]
            yield pl.DataFrame(batch)
            # Small delay to simulate real data reading
            await asyncio.sleep(0.01)

@pytest.fixture
def sample_data():
    """Generate sample data for testing."""
    return [
        {"id": i, "value": f"test_{i}", "timestamp": datetime.now()}
        for i in range(1000)
    ]

@pytest.fixture
def mock_home(sample_data):
    """Create a mock home instance."""
    return MockHome("test_home", sample_data, batch_size=100)

@pytest.mark.asyncio
async def test_home_initialization():
    """Test home initialization with different options."""
    # Test default initialization
    home = MockHome("test", [])
    assert home.name == "test"
    assert home.batch_size == 10_000
    assert home.row_multiplier == 300_000

    # Test custom options
    home = MockHome("test", [], batch_size=5000, row_multiplier=100_000)
    assert home.batch_size == 5000
    assert home.row_multiplier == 100_000

@pytest.mark.asyncio
async def test_read(mock_home, sample_data):
    """Test reading data in batches."""
    total_rows = 0
    batches = []

    async for df in mock_home.read():
        assert isinstance(df, pl.DataFrame)
        total_rows += len(df)
        batches.append(df)

    # Verify all data was read
    assert total_rows == len(sample_data)
    assert mock_home.iterator_called

    # Verify batch sizes
    for i, batch in enumerate(batches[:-1]):  # All except last batch
        assert len(batch) == mock_home.batch_size

    # Last batch might be smaller
    assert len(batches[-1]) <= mock_home.batch_size

@pytest.mark.asyncio
async def test_error_handling():
    """Test error handling during data reading."""
    class ErrorHome(Home):
        async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
            # Make sure we raise the error in a way that can be properly awaited
            if True:  # This ensures the error is raised in the coroutine
                raise ValueError("Test error")
            yield pl.DataFrame()  # This line is never reached

    home = ErrorHome("error_test")
    with pytest.raises(HomeError) as exc_info:
        async for _ in home.read():
            pass
    assert "Failed to read from error_test" in str(exc_info.value)

@pytest.mark.asyncio
async def test_empty_data():
    """Test handling of empty data source."""
    home = MockHome("empty_test", [])
    total_rows = 0

    async for df in home.read():
        total_rows += len(df)

    assert total_rows == 0
    assert home.iterator_called

@pytest.mark.asyncio
async def test_close_method(mock_home):
    """Test close method execution."""
    await mock_home.close()  # Should not raise any errors

    # Test custom close implementation
    class CloseableHome(MockHome):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.closed = False

        async def close(self):
            self.closed = True

    home = CloseableHome("test", [])
    await home.close()
    assert home.closed
