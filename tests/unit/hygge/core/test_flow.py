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
from typing import AsyncIterator, List, Optional
from unittest.mock import Mock

import polars as pl
import pytest

from hygge.core.flow import Flow
from hygge.core.home import Home
from hygge.core.store import Store
from hygge.utility.exceptions import ConfigError, FlowError, HomeConnectionError


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


class WatermarkCapableHome(MockHome):
    """Mock Home that supports watermark-aware reads."""

    def __init__(self, name: str, data: List[pl.DataFrame], **kwargs):
        super().__init__(name, data, **kwargs)
        self.read_with_watermark_called = False
        self.watermark_payload = None

    async def read_with_watermark(self, watermark: dict) -> AsyncIterator[pl.DataFrame]:
        """Mock incremental read implementation."""
        self.read_with_watermark_called = True
        self.watermark_payload = watermark
        async for df in super()._get_batches():
            yield df


class StubJournal:
    """Simple in-memory journal stub for Flow tests."""

    def __init__(
        self,
        watermark: Optional[dict] = None,
        exception: Optional[Exception] = None,
    ):
        self._watermark = watermark
        self._exception = exception
        self.records = []

    async def get_watermark(
        self,
        flow: str,
        entity: str,
        primary_key: Optional[str] = None,
        watermark_column: Optional[str] = None,
    ):
        if self._exception:
            raise self._exception
        return self._watermark

    async def record_entity_run(self, **kwargs):
        self.records.append(kwargs)
        return "entity_run_id"


class MockStore(Store):
    """Mock Store for testing Flow orchestration."""

    def __init__(self, name: str, **kwargs):
        super().__init__(name, kwargs)
        self.written_data: List[pl.DataFrame] = []
        self.write_called = False
        self.finish_called = False
        self.cleanup_staging_called = False
        self.cleanup_staging_call_count = 0

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

    async def cleanup_staging(self):
        """Mock cleanup_staging for testing retry logic."""
        self.cleanup_staging_called = True
        self.cleanup_staging_call_count += 1
        # Clear written data to simulate cleanup
        self.written_data.clear()


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    from datetime import datetime, timezone

    return [
        pl.DataFrame(
            {
                "id": range(100),
                "value": ["test"] * 100,
                "updated_at": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 100,
            }
        ),
        pl.DataFrame(
            {
                "id": range(100, 200),
                "value": ["test"] * 100,
                "updated_at": [datetime(2024, 1, 2, tzinfo=timezone.utc)] * 100,
            }
        ),
        pl.DataFrame(
            {
                "id": range(200, 250),
                "value": ["test"] * 50,
                "updated_at": [datetime(2024, 1, 3, tzinfo=timezone.utc)] * 50,
            }
        ),
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
        options={"queue_size": 5},
        entity_name="test_flow",
        base_flow_name="test_flow",
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
        flow = Flow(
            name="test",
            home=mock_home,
            store=mock_store,
            entity_name="test",
            base_flow_name="test",
        )
        assert flow.queue_size == 10  # Default from FlowSettings
        assert flow.timeout == 300  # Default from FlowSettings

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
        flow = Flow(
            name="empty_flow",
            home=empty_home,
            store=store,
            entity_name="empty_flow",
            base_flow_name="empty_flow",
        )

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
        flow = Flow(
            name="error_flow",
            home=error_home,
            store=store,
            entity_name="error_flow",
            base_flow_name="error_flow",
        )

        # When starting the flow
        with pytest.raises(FlowError) as exc_info:
            await flow.start()

        # Then should raise FlowError with proper message
        error_message = str(exc_info.value)
        assert "Producer failed" in error_message
        assert "Home read error" in error_message

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_incremental_uses_watermark_when_supported(self, sample_data):
        """Flow should use watermark-aware reads when home supports it."""
        home = WatermarkCapableHome("users_home", sample_data, batch_size=100)
        store = MockStore("test_store")
        journal = StubJournal(
            watermark={
                "watermark": "2024-01-01T00:00:00Z",
                "watermark_type": "datetime",
                "watermark_column": "updated_at",
                "primary_key": "id",
            }
        )
        flow = Flow(
            name="users_flow_users",
            home=home,
            store=store,
            options={"queue_size": 5},
            journal=journal,
            coordinator_run_id="coord_run",
            flow_run_id="flow_run",
            coordinator_name="coord",
            base_flow_name="users_flow",
            entity_name="users",
            run_type="incremental",
            watermark_config={"primary_key": "id", "watermark_column": "updated_at"},
        )

        await flow.start()

        assert home.read_with_watermark_called
        assert home.watermark_payload == journal._watermark
        assert journal.records, "Journal should record entity run"
        assert journal.records[-1]["message"] is None

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_incremental_watermark_mismatch_falls_back(self, sample_data):
        """Flow should fall back to full load when watermark config mismatches."""
        mismatch_error = ValueError(
            "stored watermark_column='created_at' but requested 'updated_at'"
        )
        journal = StubJournal(exception=mismatch_error)
        home = WatermarkCapableHome("users_home", sample_data, batch_size=100)
        store = MockStore("test_store")

        flow = Flow(
            name="users_flow_users",
            home=home,
            store=store,
            options={"queue_size": 5},
            journal=journal,
            coordinator_run_id="coord_run",
            flow_run_id="flow_run",
            coordinator_name="coord",
            base_flow_name="users_flow",
            entity_name="users",
            run_type="incremental",
            watermark_config={"primary_key": "id", "watermark_column": "updated_at"},
        )

        await flow.start()

        assert home.read_called
        assert not home.read_with_watermark_called
        assert journal.records, "Journal should record entity run"
        recorded_message = journal.records[-1]["message"]
        assert recorded_message == flow.watermark_message
        assert flow.watermark_message is not None

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_flow_watermark_validation_fails_fast(self, sample_data):
        """Flow should validate watermark schema on first batch and fail fast."""
        # Create data without the watermark column
        invalid_data = [
            pl.DataFrame({"id": range(100), "value": ["test"] * 100}),
        ]
        home = MockHome("users_home", invalid_data, batch_size=100)
        store = MockStore("test_store")

        flow = Flow(
            name="users_flow_users",
            home=home,
            store=store,
            options={"queue_size": 5},
            entity_name="users",
            watermark_config={"primary_key": "id", "watermark_column": "updated_at"},
        )

        # Should fail on first batch with FlowError wrapping ConfigError
        # The ConfigError is raised during validation and wrapped by the consumer
        with pytest.raises(FlowError, match="Watermark column 'updated_at' not found"):
            await flow.start()

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    async def test_flow_handles_store_error(self, sample_data):
        """Test Flow handles Store errors gracefully."""

        class ErrorStore(MockStore):
            async def write(self, df: pl.DataFrame, is_recursive: bool = False):
                raise ValueError("Store write error")

        home = MockHome("test_home", sample_data)
        error_store = ErrorStore("error_store")
        flow = Flow(
            name="error_flow",
            home=home,
            store=error_store,
            entity_name="error_flow",
            base_flow_name="error_flow",
        )

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
        flow = Flow(
            name="slow_flow",
            home=slow_home,
            store=store,
            entity_name="slow_flow",
            base_flow_name="slow_flow",
        )

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

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_retries_on_transient_connection_error(self, sample_data):
        """Test Flow retries on transient connection errors and cleans up staging."""

        # Create a home that fails with transient error, then succeeds
        attempt_count = {"count": 0}

        class RetryableHome(MockHome):
            async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
                attempt_count["count"] += 1
                if attempt_count["count"] == 1:
                    # First attempt: fail with transient connection error
                    raise HomeConnectionError(
                        "Connection error: ('08S01', "
                        "'[08S01] [Microsoft][ODBC Driver 18 for SQL Server]"
                        "TCP Provider: An existing connection was forcibly closed "
                        "by the remote host. (10054)')"
                    )
                # Second attempt: succeed
                async for df in super()._get_batches():
                    yield df

        home = RetryableHome("retry_home", sample_data)
        store = MockStore("retry_store")
        flow = Flow(
            name="retry_flow",
            home=home,
            store=store,
            entity_name="retry_flow",
            base_flow_name="retry_flow",
        )

        # When starting the flow
        await flow.start()

        # Then should have retried and succeeded
        assert attempt_count["count"] == 2
        assert flow.total_rows == sum(len(df) for df in sample_data)
        # And cleanup_staging should have been called before retry
        assert store.cleanup_staging_called
        assert store.cleanup_staging_call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_resets_state_on_retry(self, sample_data):
        """Test Flow resets state (total_rows, rows_written) before retry."""
        attempt_count = {"count": 0}

        class RetryableHome(MockHome):
            async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
                attempt_count["count"] += 1
                if attempt_count["count"] == 1:
                    # First attempt: fail after writing some data
                    yield pl.DataFrame({"id": [1, 2, 3], "value": ["test"] * 3})
                    raise HomeConnectionError(
                        "Connection error: connection forcibly closed"
                    )
                # Second attempt: succeed
                async for df in super()._get_batches():
                    yield df

        home = RetryableHome("retry_home", sample_data)
        store = MockStore("retry_store")
        flow = Flow(
            name="retry_flow",
            home=home,
            store=store,
            entity_name="retry_flow",
            base_flow_name="retry_flow",
        )

        # When starting the flow
        await flow.start()

        # Then should have retried and succeeded
        assert attempt_count["count"] == 2
        # State should be reset - should only count rows from successful attempt
        assert flow.total_rows == sum(len(df) for df in sample_data)
        # Should not have duplicate rows from first attempt
        assert len(store.written_data) == len(sample_data)

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_does_not_retry_non_transient_errors(self, sample_data):
        """Test Flow does not retry non-transient errors."""
        attempt_count = {"count": 0}

        class FailingHome(MockHome):
            async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
                attempt_count["count"] += 1
                # Always fail with non-transient error
                # Must be an async generator, so we raise before any yield
                raise ValueError("Invalid configuration")
                yield  # Unreachable, but makes it an async generator

        home = FailingHome("failing_home", sample_data)
        store = MockStore("failing_store")
        flow = Flow(
            name="failing_flow",
            home=home,
            store=store,
            entity_name="failing_flow",
            base_flow_name="failing_flow",
        )

        # When starting the flow
        with pytest.raises(FlowError):
            await flow.start()

        # Then should not have retried (only one attempt)
        assert attempt_count["count"] == 1
        # And cleanup_staging should not have been called
        assert not store.cleanup_staging_called

    @pytest.mark.asyncio
    @pytest.mark.timeout(30)
    async def test_flow_gives_up_after_max_retries(self, sample_data):
        """Test Flow gives up after max retries (3 attempts)."""
        attempt_count = {"count": 0}

        class AlwaysFailingHome(MockHome):
            async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
                attempt_count["count"] += 1
                # Always fail with transient connection error
                # Must be an async generator, so we raise before any yield
                raise HomeConnectionError(
                    "Connection error: connection forcibly closed"
                )
                yield  # Unreachable, but makes it an async generator

        home = AlwaysFailingHome("failing_home", sample_data)
        store = MockStore("failing_store")
        flow = Flow(
            name="failing_flow",
            home=home,
            store=store,
            entity_name="failing_flow",
            base_flow_name="failing_flow",
        )

        # When starting the flow
        # Tenacity wraps the exception in RetryError when retries are exhausted
        with pytest.raises(Exception) as exc_info:
            await flow.start()

        # Should raise either FlowError/HomeConnectionError or RetryError wrapping them
        last_exception = exc_info.value
        if hasattr(exc_info.value, "last_attempt"):
            last_exception = exc_info.value.last_attempt.exception()

        # Accept FlowError or HomeConnectionError (connection errors are preserved)
        assert isinstance(last_exception, (FlowError, HomeConnectionError))

        # Then should have tried 3 times (initial + 2 retries)
        assert attempt_count["count"] == 3
        # And cleanup_staging should have been called before each retry (2 times)
        assert store.cleanup_staging_call_count == 2


class TestFlowConfigSafeAccess:
    """Test FlowConfig safe config access methods (no side effects)."""

    def test_get_store_config_from_dict(self):
        """Test get_store_config() returns config without creating store."""
        from hygge.core.flow import FlowConfig

        config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
        )

        # Should return StoreConfig without creating Store instance
        store_config = config.get_store_config()
        assert store_config.type == "parquet"
        assert store_config.path == "data/dest"
        # Verify it's a StoreConfig, not a Store instance
        assert hasattr(store_config, "model_dump")
        # Store has write, StoreConfig doesn't
        assert not hasattr(store_config, "write")

    def test_get_store_config_from_string(self):
        """Test get_store_config() handles string configs."""
        from hygge.core.flow import FlowConfig

        config = FlowConfig(home="data/source", store="data/dest")

        store_config = config.get_store_config()
        assert store_config.type == "parquet"
        assert store_config.path == "data/dest"

    def test_get_home_config_from_dict(self):
        """Test get_home_config() returns config without creating home."""
        from hygge.core.flow import FlowConfig

        config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
        )

        # Should return HomeConfig without creating Home instance
        home_config = config.get_home_config()
        assert home_config.type == "parquet"
        assert home_config.path == "data/source"
        # Verify it's a HomeConfig, not a Home instance
        assert hasattr(home_config, "model_dump")
        assert not hasattr(home_config, "read")  # Home has read, HomeConfig doesn't

    def test_get_home_config_from_string(self):
        """Test get_home_config() handles string configs."""
        from hygge.core.flow import FlowConfig

        config = FlowConfig(home="data/source", store="data/dest")

        home_config = config.get_home_config()
        assert home_config.type == "parquet"
        assert home_config.path == "data/source"


class TestFlowFromConfig:
    """Test Flow.from_config() class method."""

    def test_from_config_creates_flow(self):
        """Test Flow.from_config() creates flow from configuration."""
        from hygge.core.flow import Flow, FlowConfig

        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
        )

        flow = Flow.from_config(
            flow_name="test_flow",
            flow_config=flow_config,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        assert flow.name == "test_flow"
        assert flow.home is not None
        assert flow.store is not None
        assert flow.home.config.type == "parquet"
        assert flow.store.config.type == "parquet"
        assert flow.coordinator_run_id == "test_run_id"
        assert flow.coordinator_name == "test_coordinator"
        assert flow.base_flow_name == "test_flow"
        # For non-entity flows, entity_name is set to flow_name
        assert flow.entity_name == "test_flow"

    def test_from_config_with_run_type(self):
        """Test Flow.from_config() respects run_type configuration."""
        from hygge.core.flow import Flow, FlowConfig

        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
            run_type="incremental",
        )

        flow = Flow.from_config(
            flow_name="test_flow",
            flow_config=flow_config,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        assert flow.run_type == "incremental"

    def test_from_config_with_full_drop_override(self):
        """Test Flow.from_config() handles full_drop compatibility flag."""
        from hygge.core.flow import Flow, FlowConfig

        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
            full_drop=False,  # Should override run_type to incremental
        )

        flow = Flow.from_config(
            flow_name="test_flow",
            flow_config=flow_config,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        assert flow.run_type == "incremental"

    def test_from_config_with_watermark(self):
        """Test Flow.from_config() includes watermark configuration."""
        from hygge.core.flow import Flow, FlowConfig

        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
            watermark={"primary_key": "id", "watermark_column": "updated_at"},
        )

        flow = Flow.from_config(
            flow_name="test_flow",
            flow_config=flow_config,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        expected_watermark = {"primary_key": "id", "watermark_column": "updated_at"}
        assert flow.watermark_config == expected_watermark

    def test_from_config_with_flow_overrides(self):
        """Test Flow.from_config() applies flow overrides."""
        from hygge.core.flow import Flow, FlowConfig

        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
            run_type="full_drop",
        )

        flow_overrides = {"test_flow": {"run_type": "incremental"}}

        flow = Flow.from_config(
            flow_name="test_flow",
            flow_config=flow_config,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
            flow_overrides=flow_overrides,
        )

        # Override should change run_type
        assert flow.run_type == "incremental"

    def test_from_config_open_mirroring_requires_key_columns(self):
        """Test Flow.from_config() validates Open Mirroring key_columns."""
        from hygge.core.flow import Flow, FlowConfig

        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={
                "type": "open_mirroring",
                "account_url": "https://test.dfs.fabric.microsoft.com",
                "filesystem": "test",
                "mirror_name": "test-db",
                "row_marker": 0,
                # key_columns missing - should fail
            },
        )

        with pytest.raises(ConfigError) as exc_info:
            Flow.from_config(
                flow_name="test_flow",
                flow_config=flow_config,
                coordinator_run_id="test_run_id",
                coordinator_name="test_coordinator",
                connection_pools={},
                journal_cache={},
            )

        assert "key_columns" in str(exc_info.value).lower()


class TestFlowFromEntity:
    """Test Flow.from_entity() class method."""

    def test_from_entity_creates_flow(self):
        """Test Flow.from_entity() creates entity flow from Entity."""
        from hygge.core.flow import Entity, Flow, FlowConfig

        # Create merged FlowConfig (simulating what Workspace does)
        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
        )

        entity_config = {"name": "users"}

        # Create Entity with merged config
        entity = Entity(
            flow_name="test_flow_users",
            base_flow_name="test_flow",
            entity_name="users",
            flow_config=flow_config,
            entity_config=entity_config,
        )

        flow = Flow.from_entity(
            entity,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        assert flow.name == "test_flow_users"
        assert flow.base_flow_name == "test_flow"
        assert flow.entity_name == "users"
        assert flow.home is not None
        assert flow.store is not None

    def test_from_entity_merges_home_path(self):
        """Test Flow.from_entity() uses merged entity home path."""
        from hygge.core.flow import Entity, Flow, FlowConfig
        from hygge.utility.path_helper import PathHelper

        # Create merged FlowConfig with path already merged (simulating Workspace)
        merged_path = PathHelper.merge_paths("data/source", "users")
        flow_config = FlowConfig(
            home={"type": "parquet", "path": merged_path},
            store={"type": "parquet", "path": "data/dest"},
        )

        entity_config = {
            "name": "users",
            "home": {"path": "users"},
        }

        # Create Entity with merged config
        entity = Entity(
            flow_name="test_flow_users",
            base_flow_name="test_flow",
            entity_name="users",
            flow_config=flow_config,
            entity_config=entity_config,
        )

        flow = Flow.from_entity(
            entity,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        # Path should be merged: "data/source" + "users" = "data/source/users"
        assert flow.home.config.path == "data/source/users"

    def test_from_entity_merges_store_config(self):
        """Test Flow.from_entity() uses merged entity store config."""
        from hygge.core.flow import Entity, Flow, FlowConfig

        # Create merged FlowConfig with store override already applied
        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={
                "type": "parquet",
                "path": "data/dest",
                "options": {"compression": "gzip"},  # Entity override applied
            },
        )

        entity_config = {
            "name": "users",
            "store": {"options": {"compression": "gzip"}},
        }

        # Create Entity with merged config
        entity = Entity(
            flow_name="test_flow_users",
            base_flow_name="test_flow",
            entity_name="users",
            flow_config=flow_config,
            entity_config=entity_config,
        )

        flow = Flow.from_entity(
            entity,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        # Store config should have entity override
        assert flow.store.config.options["compression"] == "gzip"

    def test_from_entity_with_entity_run_type(self):
        """Test Flow.from_entity() uses entity run_type override."""
        from hygge.core.flow import Entity, Flow, FlowConfig

        # Create merged FlowConfig with entity run_type override already applied
        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
            run_type="incremental",  # Entity override applied
        )

        entity_config = {
            "name": "users",
            "run_type": "incremental",
        }

        # Create Entity with merged config
        entity = Entity(
            flow_name="test_flow_users",
            base_flow_name="test_flow",
            entity_name="users",
            flow_config=flow_config,
            entity_config=entity_config,
        )

        flow = Flow.from_entity(
            entity,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        # Entity run_type should override flow default
        assert flow.run_type == "incremental"

    def test_from_entity_with_entity_watermark(self):
        """Test Flow.from_entity() uses entity watermark override."""
        from hygge.core.flow import Entity, Flow, FlowConfig

        # Create merged FlowConfig with entity watermark override already applied
        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
            watermark={"primary_key": "user_id", "watermark_column": "updated_at"},
        )

        entity_config = {
            "name": "users",
            "watermark": {"primary_key": "user_id", "watermark_column": "updated_at"},
        }

        # Create Entity with merged config
        entity = Entity(
            flow_name="test_flow_users",
            base_flow_name="test_flow",
            entity_name="users",
            flow_config=flow_config,
            entity_config=entity_config,
        )

        flow = Flow.from_entity(
            entity,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        # Entity watermark should override flow default
        expected_watermark = {
            "primary_key": "user_id",
            "watermark_column": "updated_at",
        }
        assert flow.watermark_config == expected_watermark

    def test_from_entity_open_mirroring_key_columns(self):
        """Test Flow.from_entity() handles Open Mirroring key_columns from entity."""
        from hygge.core.flow import Entity, Flow, FlowConfig

        # Create merged FlowConfig with entity key_columns already merged
        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={
                "type": "open_mirroring",
                "account_url": "https://test.dfs.fabric.microsoft.com",
                "filesystem": "test",
                "mirror_name": "test-db",
                "row_marker": 0,
                "key_columns": ["id"],  # Entity provides key_columns (merged)
            },
        )

        entity_config = {
            "name": "users",
            "store": {"key_columns": ["id"]},
        }

        # Create Entity with merged config
        entity = Entity(
            flow_name="test_flow_users",
            base_flow_name="test_flow",
            entity_name="users",
            flow_config=flow_config,
            entity_config=entity_config,
        )

        flow = Flow.from_entity(
            entity,
            coordinator_run_id="test_run_id",
            coordinator_name="test_coordinator",
            connection_pools={},
            journal_cache={},
        )

        # Should succeed - entity provides key_columns
        assert flow.store.config.key_columns == ["id"]


class TestFlowHelperMethods:
    """Test FlowFactory helper methods (_apply_overrides, _validate_run_type_alignment)."""  # noqa: E501

    def test_apply_overrides_deep_merge(self):
        """Test _apply_overrides() performs deep merge of configs."""
        from hygge.core.flow import FlowConfig, FlowFactory

        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={
                "type": "parquet",
                "path": "data/dest",
                "options": {"compression": "snappy"},
            },
        )

        overrides = {
            "test_flow": {
                # Deep merge into store.options
                "store": {"options": {"compression": "gzip"}},
            }
        }

        updated_config = FlowFactory._apply_overrides(
            flow_config, "test_flow", overrides
        )

        # Should merge options, not replace entire store
        assert updated_config.store["options"]["compression"] == "gzip"
        # Original path preserved
        assert updated_config.store["path"] == "data/dest"

    def test_apply_overrides_no_overrides(self):
        """Test _apply_overrides() returns original config when no overrides."""
        from hygge.core.flow import FlowConfig, FlowFactory

        flow_config = FlowConfig(
            home={"type": "parquet", "path": "data/source"},
            store={"type": "parquet", "path": "data/dest"},
        )

        # No overrides for this flow
        overrides = {"other_flow": {"run_type": "incremental"}}

        updated_config = FlowFactory._apply_overrides(
            flow_config, "test_flow", overrides
        )

        # Should return original config unchanged
        assert updated_config == flow_config

    def test_validate_run_type_alignment_warns_on_mismatch(self):
        """Test _validate_run_type_alignment() warns on mismatch."""
        from hygge.core.flow import FlowFactory
        from hygge.core.store import StoreConfig

        # Create a mock logger to capture warnings
        mock_logger = Mock()

        # Create an ADLS store config that supports incremental
        # (Parquet stores don't have incremental field)
        store_config = StoreConfig.create(
            {
                "type": "adls",
                "account_url": "https://test.dfs.core.windows.net",
                "filesystem": "test",
                "path": "data/dest",
                "incremental": True,  # Store forces incremental
            }
        )

        # Flow run_type is full_drop - should warn
        FlowFactory._validate_run_type_alignment(
            store_config, "full_drop", "test_flow", None, mock_logger
        )

        # Should have logged a warning
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0].lower()
        assert "incremental" in warning_msg or "full_drop" in warning_msg
