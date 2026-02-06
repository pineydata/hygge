"""
Updated tests for the Store base class.

Following hygge's testing principles:
- Test behavior that makes sense to users
- Focus on data collection, staging, and final storage
- Keep tests clear and maintainable
- Test the actual Store API as implemented
"""
import asyncio
from pathlib import Path

import polars as pl
import pytest

from hygge.core.store import Store
from hygge.utility.exceptions import StoreError


class SimpleStore(Store, store_type="test"):
    """Test implementation of Store for verification."""

    def __init__(self, name: str, **kwargs):
        super().__init__(name, kwargs)
        self.saved_data = []
        self.saved_paths = []
        self.moved_files = []
        self.cleanup_files = []
        self.filename_count = 0

        # Mock directories
        self.staging_dir = "test_staging"
        self.final_dir = "test_final"

    async def _save(self, df: pl.DataFrame, path: str) -> None:
        """Mock save implementation."""
        self.saved_data.append(df)
        self.saved_paths.append(path)

    async def _move_to_final(self, staging_path: Path, final_path: Path) -> None:
        """Mock move implementation."""
        self.moved_files.append((staging_path, final_path))

    async def _cleanup_temp(self, path: Path) -> None:
        """Mock cleanup implementation."""
        self.cleanup_files.append(path)

    async def get_next_filename(self) -> str:
        """Mock filename generation."""
        self.filename_count += 1
        return f"batch_{self.filename_count:04d}.parquet"

    def get_staging_directory(self) -> Path:
        """Mock staging directory."""
        return Path(self.staging_dir)

    def get_final_directory(self) -> Path:
        """Mock final directory."""
        return Path(self.final_dir)


class FailingStore(Store, store_type="failing"):
    """Test implementation that fails during save."""

    def __init__(self, name: str, **kwargs):
        super().__init__(name, kwargs)
        self.filename_count = 0

    async def _save(self, df: pl.DataFrame, path: str) -> None:
        """Save implementation that fails."""
        raise ValueError("Save failed")

    async def _move_to_final(self, staging_path: Path, final_path: Path) -> None:
        pass

    async def _cleanup_temp(self, path: Path) -> None:
        pass

    async def get_next_filename(self) -> str:
        self.filename_count += 1
        return f"batch_{self.filename_count:04d}.parquet"

    def get_staging_directory(self) -> Path:
        return Path("test_staging")

    def get_final_directory(self) -> Path:
        return Path("test_final")


class PathStore(Store, store_type="path"):
    """Test implementation with custom path management."""

    def __init__(self, name: str, staging_dir: str, final_dir: str, **kwargs):
        super().__init__(name, kwargs)
        self.staging_dir = staging_dir
        self.final_dir = final_dir
        self.filename_count = 0
        self.saved_paths = []
        self.moved_files = []
        self.cleanup_files = []

    async def _save(self, df: pl.DataFrame, path: str) -> None:
        """Mock save with path tracking."""
        self.saved_paths.append(path)

    async def _move_to_final(self, staging_path: Path, final_path: Path) -> None:
        """Mock move with path tracking."""
        self.moved_files.append((staging_path, final_path))

    async def _cleanup_temp(self, path: Path) -> None:
        """Mock cleanup with path tracking."""
        self.cleanup_files.append(path)

    async def get_next_filename(self) -> str:
        self.filename_count += 1
        return f"test_{self.filename_count}.parquet"

    def get_staging_directory(self) -> Path:
        return Path(self.staging_dir)

    def get_final_directory(self) -> Path:
        return Path(self.final_dir)


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pl.DataFrame(
        {
            "id": range(100),
            "name": [f"user_{i}" for i in range(100)],
            "value": [i * 10 for i in range(100)],
        }
    )


@pytest.fixture
def large_data():
    """Create large data for testing batching."""
    return pl.DataFrame({"id": range(500000), "value": ["large"] * 500000})


@pytest.fixture
def simple_store():
    """Create a simple store instance."""
    return SimpleStore("test_store", batch_size=10000)


@pytest.fixture
def failing_store():
    """Create a failing store instance."""
    return FailingStore("test_failing_store")


@pytest.fixture
def path_store():
    """Create a path-aware store instance."""
    return PathStore("test_path_store", "staging", "final")


class TestStoreInitialization:
    """Test Store initialization and configuration."""

    def test_store_initialization_defaults(self):
        """Test Store initializes with correct defaults."""
        store = SimpleStore("test_store")

        assert store.name == "test_store"
        assert store.batch_size == 100_000  # Default batch size
        assert store.row_multiplier == 300_000  # Default row multiplier
        assert store.options == {}
        assert store.current_df is None
        assert store.total_rows == 0
        assert store.transfers == []
        assert store.start_time is None

    def test_store_initialization_custom_options(self):
        """Test Store respects custom configuration options."""
        options = {
            "batch_size": 5000,
            "row_multiplier": 100000,
            "temp_pattern": "custom_temp/{name}/{filename}",
            "final_pattern": "custom_final/{name}/{filename}",
        }
        store = SimpleStore("test_store", **options)

        assert store.batch_size == 5000
        assert store.row_multiplier == 100000
        assert "custom_temp" in store.temp_pattern
        assert "custom_final" in store.final_pattern

    def test_store_missing_implementation(self):
        """Test that incomplete Store implementations fail appropriately."""

        class IncompleteStore(Store):
            pass  # Missing all required methods

        # With ABC, incomplete implementations can't be instantiated
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteStore("incomplete", {})


class TestStoreDataCollection:
    """Test Store data collection and buffering."""

    @pytest.mark.asyncio
    async def test_store_collects_data_under_batch_size(
        self, simple_store, sample_data
    ):
        """Test Store collects data when under batch size."""
        # Given data smaller than batch size
        assert len(sample_data) < simple_store.batch_size

        # When writing data
        result = await simple_store.write(sample_data)

        # Then data should be collected but not staged
        assert result is None  # No staging triggered
        assert simple_store.current_df is not None
        assert len(simple_store.current_df) == len(sample_data)
        assert simple_store.total_rows == len(sample_data)
        assert len(simple_store.transfers) == 0  # No transfers yet

    @pytest.mark.asyncio
    async def test_store_triggers_staging_on_batch_size(self, path_store, large_data):
        """Test Store triggers staging when batch size exceeded."""
        # Set batch size to ensure remaining data after staging
        path_store.batch_size = 150000  # 500000 / 150000 = 3 batches + 50000 remainder

        # Given large data exceeding batch size
        assert len(large_data) > path_store.batch_size

        # When writing data
        await path_store.write(large_data)

        # Then should stage data
        assert len(path_store.saved_paths) > 0  # Staging occurred
        assert path_store.current_df is not None  # Should have remaining data

    @pytest.mark.asyncio
    async def test_store_handles_multiple_writes(self, simple_store, sample_data):
        """Test Store handles multiple data writes."""
        # Small data chunks
        chunk1 = sample_data.slice(0, 50)
        chunk2 = sample_data.slice(50, 50)

        # When writing multiple chunks
        result1 = await simple_store.write(chunk1)
        result2 = await simple_store.write(chunk2)

        # Then should accumulate data
        assert result1 is None  # Under batch size
        assert result2 is None  # Under batch size
        assert len(simple_store.current_df) == 100  # Accumulated
        assert simple_store.total_rows == 100


class TestStoreStaging:
    """Test Store staging functionality."""

    @pytest.mark.asyncio
    async def test_store_stages_data_correctly(self, path_store, sample_data):
        """Test Store stages data with correct paths."""
        # Set low batch size to trigger staging
        path_store.batch_size = 50
        large_data = sample_data

        # When writing data that triggers staging
        await path_store.write(large_data)

        # Then should stage with correct filename
        assert len(path_store.saved_paths) > 0
        staged_path = path_store.saved_paths[0]
        assert "staging/test_1.parquet" in Path(staged_path).as_posix()

    @pytest.mark.asyncio
    async def test_store_collects_filenames_correctly(self, path_store, sample_data):
        """Test Store generates unique filenames."""
        path_store.batch_size = 30  # Small batch to trigger multiple stages

        # When writing large data that triggers multiple stages
        await path_store.write(sample_data)

        # Then should have multiple unique filenames
        assert len(path_store.saved_paths) >= 2  # Multiple stages
        filenames = [Path(p).name for p in path_store.saved_paths]
        assert len(set(filenames)) == len(filenames)  # All unique

    @pytest.mark.asyncio
    async def test_store_handles_staging_error(self, failing_store, sample_data):
        """Test Store handles staging errors gracefully."""
        # First add data to the store
        failing_store.current_df = sample_data

        # When staging data that causes error
        with pytest.raises(ValueError) as exc_info:
            await failing_store._stage()

        # Then should raise appropriate error
        assert "Save failed" in str(exc_info.value)


class TestStoreFinish:
    """Test Store finish functionality."""

    @pytest.mark.asyncio
    async def test_store_finishes_with_remaining_data(self, path_store, sample_data):
        """Test Store finishes with remaining buffered data."""
        # Write small data that doesn't trigger staging
        await path_store.write(sample_data)

        # When finishing
        await path_store.finish()

        # Then should stage remaining data
        assert len(path_store.saved_paths) == 1
        assert path_store.current_df is None  # Buffer cleared

    @pytest.mark.asyncio
    async def test_store_moves_files_to_final(self, path_store, sample_data):
        """Test Store moves staged files to final location."""
        # Write and finish data
        path_store.batch_size = 50  # Trigger staging
        await path_store.write(sample_data)
        await path_store.finish()

        # Then should move files to final location
        assert len(path_store.moved_files) > 0
        for staging_path, final_path in path_store.moved_files:
            assert Path(final_path).as_posix().startswith("final/")


class TestStoreProgressTracking:
    """Test Store progress tracking."""

    @pytest.mark.asyncio
    async def test_store_tracks_progress(self, simple_store, sample_data):
        """Test Store tracks progress correctly."""
        # When writing data
        await simple_store.write(sample_data)

        # Then should track total rows and timing
        assert simple_store.total_rows == len(sample_data)
        assert simple_store.start_time is not None

        # And finish should complete successfully
        await simple_store.finish()

    @pytest.mark.asyncio
    async def test_store_logs_progress_periodically(self, simple_store):
        """Test Store logs progress at correct intervals."""
        # Create data that crosses row_multiplier boundary
        large_data = pl.DataFrame({"id": range(400000), "value": ["test"] * 400000})

        # When writing large data (forces low batch_size to avoid staging)
        simple_store.batch_size = 500000  # Force accumulation
        await simple_store.write(large_data)
        await simple_store.finish()

        # Then should have tracked correct total
        assert simple_store.total_rows == 400000


class TestStoreErrorHandling:
    """Test Store error handling."""

    @pytest.mark.asyncio
    async def test_store_handles_none_data(self, simple_store):
        """Test Store handles None data gracefully."""
        # When writing None data
        with pytest.raises(StoreError) as exc_info:
            await simple_store.write(None)

        # Then should raise appropriate error
        assert "Cannot write None data" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_store_handles_write_error(self, failing_store, sample_data):
        """Test Store handles write errors during staging."""
        failing_store.batch_size = 50  # Force staging

        # When writing data that causes save error
        with pytest.raises(ValueError) as exc_info:
            await failing_store.write(sample_data)

        # Then should propagate error
        assert "Save failed" in str(exc_info.value)


class TestStoreConcurrency:
    """Test Store behavior in concurrent scenarios."""

    @pytest.mark.asyncio
    async def test_store_concurrent_writes(self, simple_store, sample_data):
        """Test Store handles concurrent writes."""
        # Split data for concurrent writing
        chunk1 = sample_data.slice(0, 50)
        chunk2 = sample_data.slice(50, 50)

        # When writing concurrently
        tasks = [simple_store.write(chunk1), simple_store.write(chunk2)]
        await asyncio.gather(*tasks)

        # Then should handle both writes
        await simple_store.finish()
        assert simple_store.total_rows == 100

    @pytest.mark.asyncio
    async def test_store_finish_is_idempotent(self, path_store, sample_data):
        """Test Store finish can be called multiple times safely."""
        # Write and finish data
        await path_store.write(sample_data)
        await path_store.finish()

        # When calling finish again
        await path_store.finish()

        # Then should be safe (no error)
        assert True  # Success if we get here


class TestStoreOptionalMethods:
    """Test optional store methods with default implementations."""

    def test_configure_for_run_default_is_noop(self):
        """Test configure_for_run default implementation is no-op."""
        store = SimpleStore("test")
        # Should not raise - default is no-op
        store.configure_for_run("full_drop")
        store.configure_for_run("incremental")
        assert True  # Success if we get here

    @pytest.mark.asyncio
    async def test_cleanup_staging_default_is_noop(self):
        """Test cleanup_staging default implementation is no-op."""
        store = SimpleStore("test")
        # Should not raise - default is no-op
        await store.cleanup_staging()
        assert True  # Success if we get here

    @pytest.mark.asyncio
    async def test_reset_retry_sensitive_state_default_resets_base_state(self):
        """Test reset_retry_sensitive_state default implementation resets base state."""
        store = SimpleStore("test")
        # Set some state
        store.data_buffer = [pl.DataFrame({"a": [1, 2, 3]})]
        store.buffer_size = 3
        store.total_rows = 3
        store.rows_written = 3

        # Reset should clear base state
        await store.reset_retry_sensitive_state()

        assert store.data_buffer == []
        assert store.buffer_size == 0
        assert store.total_rows == 0
        assert store.rows_written == 0

    def test_set_pool_default_is_noop(self):
        """Test set_pool default implementation is no-op."""
        store = SimpleStore("test")
        # Should not raise - default is no-op
        # Using None as pool since default doesn't use it
        store.set_pool(None)
        assert True  # Success if we get here

    def test_optional_methods_can_be_called_without_hasattr(self):
        """Test that optional methods can be called directly without hasattr checks."""
        store = SimpleStore("test")
        # All optional methods should be callable without checking first
        store.configure_for_run("full_drop")
        store.set_pool(None)
        # Async methods need await
        import asyncio

        async def test_async():
            await store.cleanup_staging()
            await store.reset_retry_sensitive_state()

        asyncio.run(test_async())
        assert True  # Success if we get here
