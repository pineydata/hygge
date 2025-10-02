"""
Tests for the simplified Flow class.

Following hygge's testing principles:
- Test behavior that matters to users
- Focus on data integrity and reliability
- Keep tests clear and maintainable
- Test the simplified Flow orchestration
"""
import asyncio
from pathlib import Path
from typing import AsyncIterator, List

import polars as pl
import pytest

from hygge.core.flow import Flow
from hygge.core.home import Home
from hygge.core.store import Store
from hygge.utility.exceptions import FlowError


class MockHome(Home):
    """Mock Home for testing Flow orchestration."""

    def __init__(self, name: str, data: List[pl.DataFrame], **kwargs):
        super().__init__(name, kwargs)
        self.data = data
        self.read_called = False

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Mock implementation that returns predefined data in batches."""
        self.read_called = True
        for df in self.data:
            yield df
            # Small delay to simulate real data reading
            await asyncio.sleep(0.01)

    def get_data_path(self):
        return Path(f"/mock/home/{self.name}")


class MockStore(Store):
    """Mock Store for testing Flow orchestration."""

    def __init__(self, name: str, **kwargs):
        super().__init__(name, kwargs)
        self.written_data: List[pl.DataFrame] = []
        self.write_called = False
        self.finish_called = False

    async def write(self, df: pl.DataFrame, is_recursive: bool = False):
        """Mock implementation that collects written data."""
        self.write_called = True
        self.written_data.append(df)
        return None  # No staging for this mock

    async def finish(self):
        """Mock implementation that tracks finish calls."""
        self.finish_called = True

    async def _save(self, df, path):
        pass  # Not used in this mock

    def get_staging_directory(self):
        return f"/mock/staging/{self.name}"

    def get_final_directory(self):
        return f"/mock/final/{self.name}"

    async def get_next_filename(self):
        return "test.parquet"

    async def _move_to_final(self, staging_path, final_path):
        pass  # Not used in this mock

    async def _cleanup_temp(self, path):
        pass  # Not used in this mock


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return [
        pl.DataFrame({"id": range(100), "value": ["test"] * 100}),
        pl.DataFrame({"id": range(100, 200), "value": ["test"] * 100}),
        pl.DataFrame({"id": range(200, 250), "value": ["test"] * 50}),
    ]


@pytest.fixture
def mock_home(sample_data):
    """Create a mock home instance."""
    return MockHome("test_home", sample_data, batch_size=100)


@pytest.fixture
def mock_store():
    """Create a mock store instance."""
    return MockStore("test_store", batch_size=150)


@pytest.fixture
def flow(mock_home, mock_store):
    """Create a Flow instance with mock Home and Store."""
    return Flow(
        name="test_flow",
        home=mock_home,
        store=mock_store,
        options={"queue_size": 5}
    )


class TestSimplifiedFlow:
    """Test suite for the simplified Flow class."""

    def test_flow_initialization(self, flow, mock_home, mock_store):
        """Test Flow initializes correctly with Home and Store."""
        assert flow.name == "test_flow"
        assert flow.home == mock_home
        assert flow.store == mock_store
        assert flow.queue_size == 5
        assert flow.total_rows == 0
        assert flow.batches_processed == 0
        assert flow.start_time is None

    def test_flow_default_options(self, mock_home, mock_store):
        """Test Flow uses default options when none provided."""
        flow = Flow(name="test", home=mock_home, store=mock_store)
        assert flow.queue_size == 10  # Default from FlowDefaults
        assert flow.timeout == 300  # Default from FlowDefaults

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_orchestration(self, flow, sample_data):
        """Test Flow orchestrates data movement correctly."""
        # When starting the flow
        await flow.start()

        # Then Home should have read data
        assert flow.home.read_called

        # And Store should have written data
        assert flow.store.write_called
        assert flow.store.finish_called

        # And Flow should track progress
        expected_rows = sum(len(df) for df in sample_data)
        assert flow.total_rows == expected_rows
        assert flow.batches_processed == len(sample_data)
        assert flow.start_time is not None

        # And all data should be written to store
        assert len(flow.store.written_data) == len(sample_data)

        # Verify data integrity
        for i, df in enumerate(flow.store.written_data):
            assert df.equals(sample_data[i])

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_handles_empty_data(self):
        """Test Flow handles empty data source gracefully."""
        # Given empty data
        empty_home = MockHome("empty_home", [])
        store = MockStore("test_store")
        flow = Flow(name="empty_flow", home=empty_home, store=store)

        # When starting the flow
        await flow.start()

        # Then should complete successfully
        assert flow.home.read_called
        assert flow.store.finish_called
        assert flow.total_rows == 0
        assert flow.batches_processed == 0
        assert len(flow.store.written_data) == 0

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_flow_handles_home_error(self):
        """Test Flow handles Home errors gracefully."""
        class ErrorHome(MockHome):
            async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
                """Simulate error by raising exception before any data."""
                raise ValueError("Home read error")
                yield  # This ensures it's recognized as a generator

        error_home = ErrorHome("error_home", [])
        store = MockStore("test_store")
        flow = Flow(name="error_flow", home=error_home, store=store)

        # When starting the flow
        with pytest.raises(FlowError) as exc_info:
            await flow.start()

        # Then should raise FlowError with proper message
        error_message = str(exc_info.value)
        assert "Producer failed" in error_message
        assert "Home read error" in error_message

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_flow_handles_store_error(self, sample_data):
        """Test Flow handles Store errors gracefully."""
        class ErrorStore(MockStore):
            async def write(self, df: pl.DataFrame, is_recursive: bool = False):
                raise ValueError("Store write error")

        home = MockHome("test_home", sample_data)
        error_store = ErrorStore("error_store")
        flow = Flow(name="error_flow", home=home, store=error_store)

        # When starting the flow
        with pytest.raises(FlowError) as exc_info:
            await flow.start()

        # Then should raise FlowError with proper message
        error_message = str(exc_info.value)
        assert "Consumer failed" in error_message or "Flow failed" in error_message
        assert "Store write error" in error_message

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_progress_tracking(self, flow, sample_data):
        """Test Flow tracks progress correctly."""
        # When starting the flow
        await flow.start()

        # Then should track progress accurately
        expected_rows = sum(len(df) for df in sample_data)
        assert flow.total_rows == expected_rows
        assert flow.batches_processed == len(sample_data)

        # And should have reasonable timing
        assert flow.start_time is not None
        duration = asyncio.get_event_loop().time() - flow.start_time
        assert duration > 0  # Should take some time due to async delays

    @pytest.mark.asyncio
    @pytest.mark.timeout(15)
    async def test_flow_cancellation_handling(self):
        """Test Flow handles cancellation gracefully."""
        # Create a slow home that we can cancel
        class SlowHome(MockHome):
            async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
                for i in range(10):  # Many batches to give time to cancel
                    yield pl.DataFrame({"id": [i], "value": ["test"]})
                    await asyncio.sleep(0.1)  # Longer delay

        slow_home = SlowHome("slow_home", [])
        store = MockStore("test_store")
        flow = Flow(name="slow_flow", home=slow_home, store=store)

        # Start the flow in a task
        task = asyncio.create_task(flow.start())

        # Cancel it after a short delay
        await asyncio.sleep(0.05)
        task.cancel()

        # Should handle cancellation gracefully
        with pytest.raises(asyncio.CancelledError):
            await task


    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_producer_consumer_pattern(self, flow, sample_data):
        """Test Flow implements producer-consumer pattern correctly."""
        # When starting the flow (using original implementation)
        await flow.start()

        # Then should complete successfully
        assert flow.total_rows == sum(len(df) for df in sample_data)
        assert flow.batches_processed == len(sample_data)
