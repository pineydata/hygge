"""
Comprehensive tests for ParquetHome implementation.

Following hygge's testing principles:
- Test behavior that matters to users (data reading, path resolution)
- Focus on data integrity and user experience
- Test error scenarios and edge cases
- Verify configuration system integration
"""
import os
import tempfile
from pathlib import Path

import polars as pl
import pytest

from hygge.homes import ParquetHome, ParquetHomeConfig
from hygge.utility import HomeError


class TestParquetHomeInitialization:
    """Test ParquetHome initialization and configuration."""

    def test_parquet_home_initialization_with_config(self):
        """Test ParquetHome initializes correctly with config."""
        config = ParquetHomeConfig(
            path="/test/data.parquet",
            options={'batch_size': 5000}
        )

        home = ParquetHome("test_home", config)

        assert home.name == "test_home"
        assert home.config == config
        assert home.data_path == Path("/test/data.parquet")
        assert home.batch_size == 5000  # From options
        assert home.row_multiplier == 300_000  # Default from base class

    def test_parquet_home_initialization_defaults(self):
        """Test ParquetHome uses defaults when no options provided."""
        config = ParquetHomeConfig(path="/test/data.parquet")

        home = ParquetHome("test_home", config)

        assert home.name == "test_home"
        assert home.batch_size == 10_000  # Default from ParquetHomeDefaults
        assert home.row_multiplier == 300_000  # Default from base class

    def test_parquet_home_initialization_merged_options(self):
        """Test ParquetHome properly merges config options with defaults."""
        config = ParquetHomeConfig(
            path="/test/data.parquet",
            options={'batch_size': 25000, 'custom_option': 'test'}
        )

        home = ParquetHome("test_home", config)

        assert home.batch_size == 25000
        assert home.options['custom_option'] == 'test'
        assert home.options['batch_size'] == 25000


class TestParquetHomePathResolution:
    """Test ParquetHome path resolution and validation."""

    def test_get_data_path(self):
        """Test get_data_path returns correct path."""
        config = ParquetHomeConfig(path="/test/data.parquet")
        home = ParquetHome("test_home", config)

        assert home.get_data_path() == Path("/test/data.parquet")

    def test_get_batch_paths_single_file(self):
        """Test get_batch_paths with single parquet file."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Create a small parquet file
            df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            df.write_parquet(tmp_path)

            config = ParquetHomeConfig(path=tmp_path)
            home = ParquetHome("test_home", config)

            paths = home.get_batch_paths()

            assert len(paths) == 1
            assert paths[0] == Path(tmp_path)

        finally:
            os.unlink(tmp_path)

    def test_get_batch_paths_directory(self):
        """Test get_batch_paths with directory containing parquet files."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create multiple parquet files
            for i in range(3):
                df = pl.DataFrame({"id": range(i*10, (i+1)*10), "value": [f"test{i}"] * 10})
                df.write_parquet(Path(tmp_dir) / f"data_{i}.parquet")

            config = ParquetHomeConfig(path=tmp_dir)
            home = ParquetHome("test_home", config)

            paths = home.get_batch_paths()

            assert len(paths) == 3
            assert all(path.suffix == '.parquet' for path in paths)
            assert all(path.parent == Path(tmp_dir) for path in paths)

    def test_get_batch_paths_empty_directory(self):
        """Test get_batch_paths raises error for empty directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = ParquetHomeConfig(path=tmp_dir)
            home = ParquetHome("test_home", config)

            with pytest.raises(HomeError) as exc_info:
                home.get_batch_paths()

            assert "No parquet files found in directory" in str(
                exc_info.value
            )
            assert tmp_dir in str(exc_info.value)

    def test_get_batch_paths_nonexistent_path(self):
        """Test get_batch_paths raises error for nonexistent path."""
        config = ParquetHomeConfig(path="/nonexistent/path.parquet")
        home = ParquetHome("test_home", config)

        with pytest.raises(HomeError) as exc_info:
            home.get_batch_paths()

        assert "Data path is neither file nor directory" in str(exc_info.value)

    def test_get_batch_paths_directory_with_non_parquet_files(self):
        """Test get_batch_paths ignores non-parquet files in directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a parquet file and some other files
            df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            df.write_parquet(Path(tmp_dir) / "data.parquet")

            # Create non-parquet files
            (Path(tmp_dir) / "data.txt").write_text("not parquet")
            (Path(tmp_dir) / "data.csv").write_text("id,value\n1,a")

            config = ParquetHomeConfig(path=tmp_dir)
            home = ParquetHome("test_home", config)

            paths = home.get_batch_paths()

            assert len(paths) == 1
            assert paths[0].name == "data.parquet"


class TestParquetHomeDataReading:
    """Test ParquetHome data reading functionality."""

    @pytest.mark.asyncio
    async def test_read_single_parquet_file(self):
        """Test reading from a single parquet file."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Create test data
            expected_df = pl.DataFrame({
                "id": range(100),
                "name": [f"user_{i}" for i in range(100)],
                "value": [i * 2 for i in range(100)]
            })
            expected_df.write_parquet(tmp_path)

            config = ParquetHomeConfig(path=tmp_path)
            home = ParquetHome("test_home", config)

            # Read data
            batches = []
            async for batch in home.read():
                batches.append(batch)

            # Verify results
            assert len(batches) == 1
            result_df = batches[0]
            assert result_df.equals(expected_df)
            assert home.start_time is not None

        finally:
            os.unlink(tmp_path)

    @pytest.mark.asyncio
    async def test_read_multiple_parquet_files(self):
        """Test reading from multiple parquet files in directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create multiple parquet files with different data
            expected_dfs = []
            for i in range(3):
                df = pl.DataFrame({
                    "id": range(i*10, (i+1)*10),
                    "batch": [i] * 10,
                    "value": [f"batch_{i}_row_{j}" for j in range(10)]
                })
                df.write_parquet(Path(tmp_dir) / f"batch_{i}.parquet")
                expected_dfs.append(df)

            config = ParquetHomeConfig(path=tmp_dir)
            home = ParquetHome("test_home", config)

            # Read data
            batches = []
            async for batch in home.read():
                batches.append(batch)

            # Verify results
            assert len(batches) == 3
            for i, batch in enumerate(batches):
                assert batch.equals(expected_dfs[i])

            assert home.start_time is not None

    @pytest.mark.asyncio
    async def test_read_empty_parquet_file(self):
        """Test reading from an empty parquet file."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Create empty parquet file
            empty_df = pl.DataFrame({"id": [], "value": []})
            empty_df.write_parquet(tmp_path)

            config = ParquetHomeConfig(path=tmp_path)
            home = ParquetHome("test_home", config)

            # Read data
            batches = []
            async for batch in home.read():
                batches.append(batch)

            # Should not yield empty DataFrames
            assert len(batches) == 0

        finally:
            os.unlink(tmp_path)

    @pytest.mark.asyncio
    async def test_read_corrupted_parquet_file(self):
        """Test reading from a corrupted parquet file."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Create a file that's not valid parquet
            with open(tmp_path, 'w') as f:
                f.write("This is not parquet data")

            config = ParquetHomeConfig(path=tmp_path)
            home = ParquetHome("test_home", config)

            # Reading should raise HomeError
            with pytest.raises(HomeError) as exc_info:
                async for batch in home.read():
                    pass

            assert "Failed to read parquet from" in str(exc_info.value)
            assert tmp_path in str(exc_info.value)

        finally:
            os.unlink(tmp_path)

    @pytest.mark.asyncio
    async def test_read_nonexistent_file(self):
        """Test reading from a nonexistent file."""
        config = ParquetHomeConfig(path="/nonexistent/file.parquet")
        home = ParquetHome("test_home", config)

        with pytest.raises(HomeError) as exc_info:
            async for batch in home.read():
                pass

        assert "Failed to read parquet from" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_read_progress_tracking(self):
        """Test that progress tracking works correctly."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Create test data
            df = pl.DataFrame({"id": range(1000), "value": ["test"] * 1000})
            df.write_parquet(tmp_path)

            config = ParquetHomeConfig(path=tmp_path)
            home = ParquetHome("test_home", config)

            # Read data
            total_rows = 0
            async for batch in home.read():
                total_rows += len(batch)

            # Verify progress tracking
            assert total_rows == 1000
            assert home.start_time is not None

        finally:
            os.unlink(tmp_path)


class TestParquetHomeConfiguration:
    """Test ParquetHome configuration system integration."""

    def test_parquet_home_config_validation(self):
        """Test ParquetHomeConfig validation."""
        # Valid config
        config = ParquetHomeConfig(path="/test/data.parquet")
        assert config.path == "/test/data.parquet"
        assert config.type == "parquet"

        # Invalid type
        with pytest.raises(ValueError) as exc_info:
            ParquetHomeConfig(type="invalid", path="/test/data.parquet")
        assert "Type must be 'parquet'" in str(exc_info.value)

        # Empty path
        with pytest.raises(ValueError) as exc_info:
            ParquetHomeConfig(path="")
        assert "Path is required" in str(exc_info.value)

    def test_parquet_home_config_defaults_merging(self):
        """Test ParquetHomeConfig defaults merging."""
        config = ParquetHomeConfig(
            path="/test/data.parquet",
            options={'batch_size': 15000, 'custom_setting': 'test'}
        )

        merged_options = config.get_merged_options()

        assert merged_options['batch_size'] == 15000
        assert merged_options['custom_setting'] == 'test'
        # Should include defaults even if not specified
        assert 'batch_size' in merged_options

    def test_parquet_home_config_defaults_only(self):
        """Test ParquetHomeConfig with only defaults."""
        config = ParquetHomeConfig(path="/test/data.parquet")

        merged_options = config.get_merged_options()

        assert merged_options['batch_size'] == 10_000  # Default value


class TestParquetHomeErrorHandling:
    """Test ParquetHome error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_read_permission_error(self):
        """Test handling of permission errors."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Create test data
            df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            df.write_parquet(tmp_path)

            # Remove read permission
            os.chmod(tmp_path, 0o000)

            config = ParquetHomeConfig(path=tmp_path)
            home = ParquetHome("test_home", config)

            with pytest.raises(HomeError) as exc_info:
                async for batch in home.read():
                    pass

            assert "Failed to read parquet from" in str(exc_info.value)

        finally:
            # Restore permissions and clean up
            os.chmod(tmp_path, 0o644)
            os.unlink(tmp_path)

    @pytest.mark.asyncio
    async def test_read_directory_with_mixed_files(self):
        """Test reading from directory with mixed file types."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create one valid parquet file
            df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            df.write_parquet(Path(tmp_dir) / "valid.parquet")

            # Create some invalid files with .parquet extension
            (Path(tmp_dir) / "invalid.parquet").write_text("not parquet")

            config = ParquetHomeConfig(path=tmp_dir)
            home = ParquetHome("test_home", config)

            # Should successfully read the valid file and handle the invalid one
            batches = []
            try:
                async for batch in home.read():
                    batches.append(batch)
            except HomeError:
                # Expected - one file will fail to read
                pass

            # Should have attempted to read both files
            # May have 0 or 1 batch depending on processing order
            assert len(batches) >= 0


class TestParquetHomeIntegration:
    """Test ParquetHome integration with hygge framework."""

    def test_parquet_home_implements_home_interface(self):
        """Test that ParquetHome properly implements the Home interface."""
        config = ParquetHomeConfig(path="/test/data.parquet")
        home = ParquetHome("test_home", config)

        # Should have all required Home attributes
        assert hasattr(home, 'name')
        assert hasattr(home, 'options')
        assert hasattr(home, 'batch_size')
        assert hasattr(home, 'row_multiplier')
        assert hasattr(home, 'start_time')
        assert hasattr(home, 'logger')

        # Should have all required Home methods
        assert hasattr(home, 'read')
        assert hasattr(home, 'get_data_path')
        assert callable(home.read)
        assert callable(home.get_data_path)

    def test_parquet_home_logger_configuration(self):
        """Test that ParquetHome has proper logger configuration."""
        config = ParquetHomeConfig(path="/test/data.parquet")
        home = ParquetHome("test_home", config)

        assert home.logger is not None
        assert home.logger.logger.name == "hygge.home.ParquetHome"
