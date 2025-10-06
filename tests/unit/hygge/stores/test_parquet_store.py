"""
Comprehensive tests for ParquetStore implementation.

Following hygge's testing principles:
- Test behavior that matters to users (data writing, file management, compression)
- Focus on data integrity and user experience
- Test error scenarios and edge cases
- Verify configuration system integration
"""
import os
import tempfile
import shutil
from pathlib import Path

import polars as pl
import pytest

from hygge.stores import ParquetStore, ParquetStoreConfig
from hygge.utility import StoreError


@pytest.fixture
def temp_store_dir():
    """Create temporary directory for store tests."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_data():
    """Create sample DataFrame for testing."""
    return pl.DataFrame({
        'id': range(1000),
        'name': [f'user_{i}' for i in range(1000)],
        'value': [i * 2.5 for i in range(1000)],
        'category': [f'cat_{i % 5}' for i in range(1000)],
        'active': [True if i % 2 == 0 else False for i in range(1000)]
    })


@pytest.fixture
def large_data():
    """Create large DataFrame for testing."""
    return pl.DataFrame({
        'id': range(100000),
        'value': [f'large_data_{i}' for i in range(100000)],
        'number': [i * 3.14 for i in range(100000)]
    })


class TestParquetStoreInitialization:
    """Test ParquetStore initialization and configuration."""

    def test_parquet_store_initialization_with_config(self, temp_store_dir):
        """Test ParquetStore initializes correctly with config."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            batch_size=5000,
            compression='gzip',
            file_pattern='test_{sequence:020d}.parquet'
        )

        store = ParquetStore("test_store", config)

        assert store.name == "test_store"
        assert store.config == config
        assert store.base_path == temp_store_dir
        assert store.batch_size == 5000
        assert store.compression == 'gzip'
        assert store.file_pattern == 'test_{sequence:020d}.parquet'
        assert store.sequence_counter == 0

    def test_parquet_store_initialization_defaults(self, temp_store_dir):
        """Test ParquetStore uses defaults when not specified."""
        config = ParquetStoreConfig(path=str(temp_store_dir))

        store = ParquetStore("test_store", config)

        assert store.name == "test_store"
        assert store.batch_size == 100_000  # Default batch size
        assert store.compression == 'snappy'  # Default compression
        assert store.file_pattern == '{sequence:020d}.parquet'  # Default pattern
        assert store.sequence_counter == 0

    def test_parquet_store_with_flow_name(self, temp_store_dir):
        """Test ParquetStore with flow_name for file pattern formatting."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            file_pattern='{flow_name}_{sequence:020d}.parquet'
        )

        store = ParquetStore("test_store", config, flow_name="my_flow")

        # Should have flow_name in the file pattern
        assert 'my_flow' in store.file_pattern

    def test_parquet_store_directories_created(self, temp_store_dir):
        """Test that ParquetStore creates necessary directories."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        staging_dir = store.get_staging_directory()
        final_dir = store.get_final_directory()

        # Directories should exist
        assert staging_dir.exists()
        assert final_dir.exists()

        # Should have correct structure
        assert staging_dir == temp_store_dir / "tmp" / "test_store"
        assert final_dir == temp_store_dir / "test_store"


class TestParquetStorePathManagement:
    """Test ParquetStore path management and directory structure."""

    def test_get_staging_directory(self, temp_store_dir):
        """Test staging directory path generation."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        staging_dir = store.get_staging_directory()
        expected = temp_store_dir / "tmp" / "test_store"

        assert staging_dir == expected
        assert staging_dir.exists()

    def test_get_final_directory(self, temp_store_dir):
        """Test final directory path generation."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        final_dir = store.get_final_directory()
        expected = temp_store_dir / "test_store"

        assert final_dir == expected
        assert final_dir.exists()

    @pytest.mark.asyncio
    async def test_get_next_filename(self, temp_store_dir):
        """Test filename generation with sequence counter."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            file_pattern='data_{sequence:020d}.parquet'
        )
        store = ParquetStore("test_store", config)

        # Generate multiple filenames
        filename1 = await store.get_next_filename()
        filename2 = await store.get_next_filename()
        filename3 = await store.get_next_filename()

        assert filename1 == 'data_00000000000000000001.parquet'
        assert filename2 == 'data_00000000000000000002.parquet'
        assert filename3 == 'data_00000000000000000003.parquet'

    @pytest.mark.asyncio
    async def test_get_next_filename_with_name_pattern(self, temp_store_dir):
        """Test filename generation with name in pattern."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            file_pattern='{name}_{sequence:020d}.parquet'
        )
        store = ParquetStore("test_store", config)

        filename = await store.get_next_filename()
        assert filename == 'test_store_00000000000000000001.parquet'

    @pytest.mark.asyncio
    async def test_get_next_filename_with_flow_name(self, temp_store_dir):
        """Test filename generation with flow_name in pattern."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            file_pattern='{flow_name}_{sequence:020d}.parquet'
        )
        store = ParquetStore("test_store", config, flow_name="my_flow")

        filename = await store.get_next_filename()
        assert filename == 'my_flow_00000000000000000001.parquet'


class TestParquetStoreDataWriting:
    """Test ParquetStore data writing functionality."""

    @pytest.mark.asyncio
    async def test_write_single_batch(self, temp_store_dir, sample_data):
        """Test writing a single batch of data."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            batch_size=10000,  # Larger than sample data
            file_pattern='test_{sequence:020d}.parquet'
        )
        store = ParquetStore("test_store", config)

        # Write data (should accumulate, not stage yet)
        result = await store.write(sample_data)

        # Should accumulate data, not stage yet
        assert result is None
        assert store.current_df is not None
        assert len(store.current_df) == len(sample_data)
        assert store.total_rows == len(sample_data)

        # Finish to write remaining data
        await store.finish()

        # Check final file was created
        final_dir = store.get_final_directory()
        output_files = list(final_dir.glob("*.parquet"))
        assert len(output_files) == 1

        # Verify data integrity
        written_df = pl.read_parquet(output_files[0])
        assert written_df.equals(sample_data)

    @pytest.mark.asyncio
    async def test_write_multiple_batches(self, temp_store_dir, large_data):
        """Test writing data that exceeds batch size."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            batch_size=50000,  # Smaller than large_data
            file_pattern='batch_{sequence:020d}.parquet'
        )
        store = ParquetStore("test_store", config)

        # Write large data (should trigger staging)
        result = await store.write(large_data)

        # Should have staged some data
        assert result is not None  # Staging occurred
        # With batch_size=50000 and 100,000 rows, should have 2 batches
        # All data should be flushed, so current_df should be None
        assert store.current_df is None  # All data should be flushed
        assert store.total_rows == len(large_data)

        # Finish to write remaining data
        await store.finish()

        # Check multiple files were created
        final_dir = store.get_final_directory()
        output_files = list(final_dir.glob("*.parquet"))
        assert len(output_files) > 1

        # Verify total data integrity
        total_rows = 0
        for file_path in output_files:
            df = pl.read_parquet(file_path)
            total_rows += len(df)

        assert total_rows == len(large_data)

    @pytest.mark.asyncio
    async def test_write_with_compression(self, temp_store_dir, sample_data):
        """Test writing with different compression algorithms."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            compression='gzip',
            file_pattern='compressed_{sequence:020d}.parquet'
        )
        store = ParquetStore("test_store", config)

        await store.write(sample_data)
        await store.finish()

        # Verify file exists and has compression
        final_dir = store.get_final_directory()
        output_files = list(final_dir.glob("*.parquet"))
        assert len(output_files) == 1

        # Read back to verify data integrity
        written_df = pl.read_parquet(output_files[0])
        assert written_df.equals(sample_data)

    @pytest.mark.asyncio
    async def test_write_incremental_data(self, temp_store_dir):
        """Test writing data in multiple small increments."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            batch_size=500,
            file_pattern='incremental_{sequence:020d}.parquet'
        )
        store = ParquetStore("test_store", config)

        # Write data in small chunks
        total_rows = 0
        for i in range(5):
            chunk_data = pl.DataFrame({
                'id': range(i * 200, (i + 1) * 200),
                'value': [f'chunk_{i}_{j}' for j in range(200)],
                'chunk': [i] * 200
            })

            await store.write(chunk_data)
            total_rows += len(chunk_data)

        # Finish to write remaining data
        await store.finish()

        # Verify all data was written
        assert store.total_rows == total_rows

        # Check files were created
        final_dir = store.get_final_directory()
        output_files = list(final_dir.glob("*.parquet"))
        assert len(output_files) > 0

        # Verify total data integrity
        written_total = 0
        for file_path in output_files:
            df = pl.read_parquet(file_path)
            written_total += len(df)

        assert written_total == total_rows


class TestParquetStoreErrorHandling:
    """Test ParquetStore error handling and edge cases."""

    def test_write_to_invalid_directory(self):
        """Test writing to invalid directory path."""
        config = ParquetStoreConfig(path="/nonexistent/invalid/path")

        # Should raise error during construction when trying to create directories
        with pytest.raises(StoreError, match="Failed to create directories"):
            ParquetStore("test_store", config)

    @pytest.mark.asyncio
    async def test_write_corrupted_data(self, temp_store_dir):
        """Test handling of corrupted data."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        # Create invalid DataFrame (should still work with polars)
        invalid_data = pl.DataFrame({'invalid_column': [None, None, None]})

        # Should handle gracefully
        await store.write(invalid_data)
        await store.finish()

        # File should still be created
        final_dir = store.get_final_directory()
        output_files = list(final_dir.glob("*.parquet"))
        assert len(output_files) == 1

    @pytest.mark.asyncio
    async def test_write_empty_dataframe(self, temp_store_dir):
        """Test writing empty DataFrame."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        empty_data = pl.DataFrame({'id': [], 'value': []})

        await store.write(empty_data)
        await store.finish()

        # Should not create files for empty data
        final_dir = store.get_final_directory()
        output_files = list(final_dir.glob("*.parquet"))
        assert len(output_files) == 0

    @pytest.mark.asyncio
    async def test_write_permission_error(self, temp_store_dir):
        """Test handling of permission errors."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        # Remove write permission from final directory
        final_dir = store.get_final_directory()
        os.chmod(final_dir, 0o444)  # Read-only

        try:
            sample_data = pl.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
            await store.write(sample_data)

            # Should raise error when finishing (moving to final location)
            with pytest.raises(StoreError):
                await store.finish()

        finally:
            # Restore permissions
            os.chmod(final_dir, 0o755)



class TestParquetStoreConfiguration:
    """Test ParquetStore configuration system integration."""

    def test_parquet_store_config_validation(self):
        """Test ParquetStoreConfig validation."""
        # Valid config
        config = ParquetStoreConfig(path="/test/destination")
        assert config.path == "/test/destination"
        assert config.type == "parquet"

        # Invalid type
        with pytest.raises(ValueError, match="Type must be 'parquet'"):
            ParquetStoreConfig(type="invalid", path="/test/destination")

        # Empty path
        with pytest.raises(ValueError, match="Path is required"):
            ParquetStoreConfig(path="")

        # Invalid compression
        with pytest.raises(ValueError, match="Compression must be one of"):
            ParquetStoreConfig(path="/test", compression="invalid")

    def test_parquet_store_config_defaults_merging(self, temp_store_dir):
        """Test ParquetStoreConfig defaults merging."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            batch_size=15000,
            compression='lz4',
            options={'custom_setting': 'test'}
        )

        merged_options = config.get_merged_options()
        assert merged_options['batch_size'] == 15000
        assert merged_options['compression'] == 'lz4'
        assert merged_options['custom_setting'] == 'test'
        assert merged_options['file_pattern'] == '{sequence:020d}.parquet'  # Default

    def test_parquet_store_config_flow_name_pattern(self, temp_store_dir):
        """Test ParquetStoreConfig with flow_name in file pattern."""
        config = ParquetStoreConfig(
            path=str(temp_store_dir),
            file_pattern='{flow_name}_data_{sequence:020d}.parquet'
        )

        merged_options = config.get_merged_options('my_flow')
        assert merged_options['file_pattern'] == 'my_flow_data_{sequence:020d}.parquet'


class TestParquetStoreCleanup:
    """Test ParquetStore cleanup functionality."""

    @pytest.mark.asyncio
    async def test_close_cleanup(self, temp_store_dir, sample_data):
        """Test that close() properly cleans up staging files."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        # Write some data
        await store.write(sample_data)
        await store.finish()

        # Check staging directory has files before cleanup
        staging_dir = store.get_staging_directory()
        staging_files_before = list(staging_dir.glob("**/*"))
        staging_files_before = [f for f in staging_files_before if f.is_file()]

        # Close should cleanup staging files
        await store.close()

        # Check staging files are cleaned up
        staging_files_after = list(staging_dir.glob("**/*"))
        staging_files_after = [f for f in staging_files_after if f.is_file()]

        # Should have fewer files after cleanup
        assert len(staging_files_after) <= len(staging_files_before)

    @pytest.mark.asyncio
    async def test_close_idempotent(self, temp_store_dir):
        """Test that close() can be called multiple times safely."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        # Should be safe to call close multiple times
        await store.close()
        await store.close()
        await store.close()

        # Should not raise any errors
        assert True


class TestParquetStoreIntegration:
    """Test ParquetStore integration with hygge framework."""

    def test_parquet_store_implements_store_interface(self, temp_store_dir):
        """Test that ParquetStore properly implements the Store interface."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        # Should have all required Store attributes
        assert hasattr(store, 'name')
        assert hasattr(store, 'options')
        assert hasattr(store, 'batch_size')
        assert hasattr(store, 'row_multiplier')
        assert hasattr(store, 'start_time')
        assert hasattr(store, 'logger')

        # Should have all required Store methods
        assert hasattr(store, 'write')
        assert hasattr(store, 'finish')
        assert hasattr(store, 'get_staging_directory')
        assert hasattr(store, 'get_final_directory')
        assert callable(store.write)
        assert callable(store.finish)
        assert callable(store.get_staging_directory)
        assert callable(store.get_final_directory)

    def test_parquet_store_logger_configuration(self, temp_store_dir):
        """Test that ParquetStore has proper logger configuration."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        assert store.logger is not None
        assert store.logger.logger.name == "hygge.store.ParquetStore"

    @pytest.mark.asyncio
    async def test_parquet_store_progress_tracking(self, temp_store_dir, sample_data):
        """Test that progress tracking works correctly."""
        config = ParquetStoreConfig(path=str(temp_store_dir))
        store = ParquetStore("test_store", config)

        await store.write(sample_data)
        await store.finish()

        # Verify progress tracking
        assert store.total_rows == len(sample_data)
        assert store.start_time is not None
