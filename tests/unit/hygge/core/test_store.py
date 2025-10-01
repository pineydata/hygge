"""
Tests for the Store class.

Following hygge's testing principles:
- Test behavior that matters to users
- Focus on data integrity and reliability
- Keep tests clear and maintainable
- Prioritize core flows
"""
import polars as pl
import pytest

from hygge.core.store import Store

pytestmark = pytest.mark.asyncio  # Mark all tests in this module as async tests


class TestStore:
    """
    Test suite for Store class focusing on core behaviors:
    - Data collection and batching
    - Staging and storage
    - Progress tracking
    - Error handling
    """

    class SimpleStore(Store):
        """Test implementation of Store for verification."""
        async def _save(self, df, path):
            self.saved_data = df
            self.saved_path = path

        async def _get_next_filename(self):
            return "test_batch.parquet"

        async def _move_to_final(self, temp_path, final_path):
            self.moved_from = temp_path
            self.moved_to = final_path

        async def _cleanup_temp(self, path):
            self.cleaned_up = path

        def _get_table_name(self, schema, table_name):
            return f"{schema}.{table_name}"

        def _get_temp_directory(self, schema, table_name):
            return f"temp/{schema}/{table_name}"

        def _get_final_directory(self, schema, table_name):
            return f"final/{schema}/{table_name}"

    @pytest.fixture
    def store(self):
        """Create a basic store instance for testing."""
        return self.SimpleStore("test_store")

    @pytest.fixture
    def store_with_options(self):
        """Create a store with custom options."""
        options = {
            'batch_size': 5000,
            'row_multiplier': 100000,
            'temp_pattern': 'custom_temp/{name}/{filename}',
            'final_pattern': 'custom_final/{name}/{filename}'
        }
        return self.SimpleStore("test_store", options=options)

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return pl.DataFrame({
            'id': range(100),
            'value': ['test'] * 100
        })

    async def test_store_initialization(self, store):
        """Test store initializes with correct defaults."""
        assert store.name == "test_store"
        assert store.batch_size == 10000  # Default batch size
        assert store.row_multiplier == 300000  # Default row multiplier
        assert store.current_df is None
        assert store.total_rows == 0
        assert store.transfers == []

    async def test_store_custom_options(self, store_with_options):
        """Test store respects custom configuration options."""
        assert store_with_options.batch_size == 5000
        assert store_with_options.row_multiplier == 100000
        assert 'custom_temp' in store_with_options.temp_pattern
        assert 'custom_final' in store_with_options.final_pattern

    async def test_path_patterns(self, store):
        """Test path pattern generation."""
        filename = "test.parquet"
        temp_path = store._get_temp_path(filename)
        final_path = store._get_final_path(filename)

        assert store.name in temp_path
        assert store.name in final_path
        assert filename in temp_path
        assert filename in final_path

    async def test_data_collection_under_batch_size(self, store, sample_data):
        """Test data collection when under batch size threshold."""
        # Given data smaller than batch size
        assert len(sample_data) < store.batch_size

        # When writing data
        result = await store.write(sample_data)

        # Then data should be collected but not staged
        assert result is None
        assert store.current_df is not None
        assert len(store.current_df) == len(sample_data)
        assert store.total_rows == len(sample_data)
        assert not store.transfers  # No transfers yet

    async def test_data_collection_triggers_batch(self, store, sample_data):
        """Test data collection triggers batch processing at threshold."""
        # Given store with small batch size
        store.batch_size = 50
        assert len(sample_data) > store.batch_size

        # When writing data
        result = await store.write(sample_data)

        # Then data should be staged and new data stored
        assert result is not None  # Staging occurred
        assert store.current_df is not None  # New data stored
        assert len(store.current_df) == len(sample_data)  # All data preserved
        assert store.total_rows == len(sample_data)
        assert len(store.transfers) == 1  # One batch transferred
        assert store.saved_data is not None
        assert len(store.saved_data) >= store.batch_size

    async def test_data_collection_multiple_writes(self, store):
        """Test data collection across multiple writes."""
        # Given multiple small data frames
        df1 = pl.DataFrame({'id': range(50), 'value': ['a'] * 50})
        df2 = pl.DataFrame({'id': range(50, 100), 'value': ['b'] * 50})

        # When writing data in sequence
        result1 = await store.write(df1)
        assert result1 is None  # First write under batch size

        result2 = await store.write(df2)
        assert result2 is None  # Combined still under batch size

        # Then data should be properly accumulated
        assert store.current_df is not None
        assert len(store.current_df) == 100
        assert store.total_rows == 100
        assert not store.transfers  # No transfers yet

        # Verify data integrity
        assert store.current_df['id'].to_list() == list(range(100))
        assert store.current_df['value'].to_list()[:50] == ['a'] * 50
        assert store.current_df['value'].to_list()[50:] == ['b'] * 50

    async def test_finish_writes_remaining_data(self, store, sample_data):
        """Test finish() writes remaining data regardless of batch size."""
        # Given data smaller than batch size
        assert len(sample_data) < store.batch_size
        await store.write(sample_data)

        # When finishing
        await store.finish()

        # Then all data should be written and state reset
        assert store.current_df is None
        assert store.total_rows == 0
        assert len(store.transfers) == 0  # Transfers cleared after move
        assert store.saved_data is not None
        assert len(store.saved_data) == len(sample_data)

    # Staging Tests
    async def test_stage_empty_data(self, store):
        """Test staging behavior with empty data."""
        # Given no data collected
        assert store.current_df is None

        # When staging
        result = await store._stage()

        # Then nothing should be staged
        assert result is None
        assert not store.transfers

    async def test_stage_collects_filename(self, store, sample_data):
        """Test staging gets correct filename."""
        # Given data to stage
        store.current_df = sample_data

        # When staging
        result = await store._stage()

        # Then should use expected filename pattern
        assert result == store._get_temp_path("test_batch.parquet")
        assert store.saved_path == result

    async def test_stage_saves_data(self, store, sample_data):
        """Test staging properly saves data."""
        # Given data to stage
        store.current_df = sample_data

        # When staging
        await store._stage()

        # Then data should be saved correctly
        assert store.saved_data is not None
        assert len(store.saved_data) == len(sample_data)
        assert store.current_df is None  # Buffer cleared

    async def test_stage_adds_to_journal(self, store, sample_data):
        """Test staging records in journal."""
        # Given data to stage
        store.current_df = sample_data

        # When staging
        temp_path = await store._stage()

        # Then should be recorded in journal
        assert temp_path in store.transfers
        assert len(store.transfers) == 1

    async def test_stage_handles_save_error(self, store, sample_data):
        """Test staging handles save errors gracefully."""
        # Given a store that fails to save
        class FailingStore(self.SimpleStore):
            async def _save(self, df, path):
                raise Exception("Save failed")

            async def _cleanup_temp(self, path):
                self.cleaned_up = path

        failing_store = FailingStore("failing_store")
        failing_store.current_df = sample_data

        # When staging
        with pytest.raises(Exception) as exc:
            await failing_store._stage()

        # Then should handle error and cleanup
        assert "Save failed" in str(exc.value)
        assert failing_store.cleaned_up is not None  # Cleanup was called

    # Error Handling Tests
    async def test_write_handles_concat_error(self, store):
        """Test write handles data concatenation errors."""
        # Given incompatible dataframes
        df1 = pl.DataFrame({'a': [1, 2, 3]})
        df2 = pl.DataFrame({'b': [4, 5, 6]})  # Different schema

        # When writing first dataframe
        await store.write(df1)

        # Then writing incompatible data should raise StoreError
        with pytest.raises(Exception) as exc:
            await store.write(df2)
        assert "Failed to write" in str(exc.value)

    async def test_write_handles_buffer_corruption(self, store, sample_data):
        """Test write handles buffer corruption."""
        # Given corrupted buffer state
        await store.write(sample_data)
        store.current_df = "corrupted"  # Simulate corruption with invalid type

        # When writing more data
        with pytest.raises(Exception) as exc:
            await store.write(sample_data)
        assert "Failed to write" in str(exc.value)

    async def test_finish_handles_remaining_write_error(self, store, sample_data):
        """Test finish handles errors when writing remaining data."""
        class FinishFailStore(self.SimpleStore):
            async def _stage(self):
                if getattr(self, 'staged_count', 0) > 0:
                    raise Exception("Second stage failed")
                self.staged_count = getattr(self, 'staged_count', 0) + 1
                return await super()._stage()

        failing_store = FinishFailStore("failing_store")
        failing_store.batch_size = 150  # Large enough to not trigger immediate staging

        # Given data that will need staging during finish
        data = pl.DataFrame({'id': range(100), 'value': ['test'] * 100})

        # Write should succeed without staging
        await failing_store.write(data)

        # When finishing, it should fail on stage attempt
        with pytest.raises(Exception) as exc:
            await failing_store.finish()  # This should trigger stage and fail
        assert "failed to finish" in str(exc.value).lower()

    async def test_finish_handles_move_error(self, store, sample_data):
        """Test finish handles errors when moving to final location."""
        class MoveFailStore(self.SimpleStore):
            async def _move_to_final(self, temp_path, final_path):
                raise Exception("Move failed")

        failing_store = MoveFailStore("failing_store")

        # Given data written to temp
        await failing_store.write(sample_data)

        # When finishing with move error
        with pytest.raises(Exception) as exc:
            await failing_store.finish()
        assert "failed to finish" in str(exc.value).lower()

    async def test_accumulation_under_batch_size(self, store):
        """Test accumulation when total size is under batch_size and not last batch."""
        # Given
        store.batch_size = 100
        df1 = pl.DataFrame({'id': range(30), 'value': ['a'] * 30})
        df2 = pl.DataFrame({'id': range(30, 60), 'value': ['b'] * 30})

        # When: Write first batch (not last)
        result1 = await store.write(df1, is_last_batch=False)

        # Then: Should accumulate without staging
        assert result1 is None
        assert store.current_df is not None
        assert len(store.current_df) == 30
        assert len(store.transfers) == 0

        # When: Write second batch that still fits (not last)
        result2 = await store.write(df2, is_last_batch=False)

        # Then: Should accumulate without staging
        assert result2 is None
        assert store.current_df is not None
        assert len(store.current_df) == 60  # Combined size
        assert len(store.transfers) == 0
        assert store.current_df['value'].to_list()[:30] == ['a'] * 30
        assert store.current_df['value'].to_list()[30:] == ['b'] * 30

    async def test_stage_on_batch_size_exceeded(self, store):
        """Test staging when current_df + new df would exceed batch_size."""
        # Given
        store.batch_size = 100
        df1 = pl.DataFrame({'id': range(70), 'value': ['a'] * 70})
        df2 = pl.DataFrame({'id': range(70, 110), 'value': ['b'] * 40})

        # When: Write first batch (not last)
        result1 = await store.write(df1, is_last_batch=False)

        # Then: Should accumulate without staging
        assert result1 is None
        assert store.current_df is not None
        assert len(store.current_df) == 70
        assert len(store.transfers) == 0

        # When: Write second batch that would exceed batch_size
        result2 = await store.write(df2, is_last_batch=False)

        # Then: Should stage current and keep new
        assert result2 is not None  # Returns staged path
        assert store.current_df is not None
        assert len(store.current_df) == 40  # Only new data
        assert len(store.transfers) == 1
        assert store.current_df['value'].to_list() == ['b'] * 40

    async def test_stage_on_last_batch(self, store):
        """Test staging when receiving last batch."""
        # Given
        store.batch_size = 100
        df1 = pl.DataFrame({'id': range(30), 'value': ['a'] * 30})
        df2 = pl.DataFrame({'id': range(30, 50), 'value': ['b'] * 20})

        # When: Write first batch (not last)
        await store.write(df1, is_last_batch=False)

        # Then: Should accumulate without staging
        assert len(store.transfers) == 0
        assert len(store.current_df) == 30

        # When: Write last batch
        result = await store.write(df2, is_last_batch=True)

        # Then: Should stage everything
        assert result is not None  # Returns staged path
        assert store.current_df is None  # Buffer cleared
        assert len(store.transfers) == 1
        assert len(store.saved_data) == 50  # Total size
        assert store.saved_data['value'].to_list()[:30] == ['a'] * 30
        assert store.saved_data['value'].to_list()[30:] == ['b'] * 20

    async def test_cleanup_after_partial_success(self, store, sample_data):
        """Test cleanup after partial success in multi-batch scenario."""
        class PartialFailStore(self.SimpleStore):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.cleanup_calls = []
                self.batch_size = 50  # Force multiple batches

            async def _save(self, df, path):
                if getattr(self, 'saved_count', 0) > 0:
                    raise Exception("Second save failed")
                self.saved_count = getattr(self, 'saved_count', 0) + 1
                await super()._save(df, path)

            async def _cleanup_temp(self, path):
                self.cleanup_calls.append(path)
                await super()._cleanup_temp(path)

        failing_store = PartialFailStore("failing_store")

        # Given data that will trigger staging
        data = pl.DataFrame({'id': range(60), 'value': ['test'] * 60})

        # When writing data that exceeds batch size
        with pytest.raises(Exception) as exc:
            await failing_store.write(data)  # This will trigger stage and fail

        # Then should cleanup temporary files
        assert len(failing_store.cleanup_calls) > 0
        assert "Second save failed" in str(exc.value)
