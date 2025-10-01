"""
Tests for ParquetHome implementation.
"""
import pytest
import polars as pl
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock

from hygge.core.homes import ParquetHome
from hygge.utils.exceptions import HomeError

@pytest.fixture
def sample_data():
    """Generate sample data for testing."""
    return pl.DataFrame({
        "id": range(1000),
        "value": [f"test_{i}" for i in range(1000)],
        "timestamp": [datetime.now() for _ in range(1000)]
    })

@pytest.fixture
def parquet_home(tmp_path):
    """Create a ParquetHome instance with a temporary path."""
    return ParquetHome(
        "test_data",
        path=str(tmp_path / "test.parquet"),
        options={'batch_size': 100}
    )

@pytest.mark.asyncio
async def test_parquet_home_initialization(tmp_path):
    """Test ParquetHome initialization with different options."""
    path = tmp_path / "test.parquet"

    # Test with minimal options
    home = ParquetHome("test", str(path))
    assert home.name == "test"
    assert home.path == path
    assert home.batch_size == 10_000  # default

    # Test with custom options
    home = ParquetHome(
        "test",
        str(path),
        options={'batch_size': 5000}
    )
    assert home.batch_size == 5000

@pytest.mark.asyncio
async def test_read_parquet(monkeypatch, parquet_home, sample_data):
    """Test reading from parquet file."""
    # Mock polars scan_parquet
    mock_scan = MagicMock()
    mock_scan.collect.return_value = [sample_data]
    mock_scan_parquet = MagicMock(return_value=mock_scan)
    monkeypatch.setattr(pl, "scan_parquet", mock_scan_parquet)

    batches = []
    async for df in parquet_home.read():
        batches.append(df)

    assert len(batches) == 1
    assert isinstance(batches[0], pl.DataFrame)
    assert len(batches[0]) == 1000

    # Verify polars was called correctly
    mock_scan_parquet.assert_called_once_with(parquet_home.path)

@pytest.mark.asyncio
async def test_read_nonexistent_file(tmp_path):
    """Test error handling when file doesn't exist."""
    home = ParquetHome("test", str(tmp_path / "nonexistent.parquet"))

    with pytest.raises(HomeError) as exc_info:
        async for _ in home.read():
            pass

    assert "Failed to read parquet" in str(exc_info.value)

@pytest.mark.asyncio
async def test_path_handling():
    """Test path handling and conversion."""
    # Test with string path
    home = ParquetHome("test", "data/test.parquet")
    assert isinstance(home.path, Path)
    assert str(home.path) == "data/test.parquet"

    # Test with Path object
    path = Path("data/test.parquet")
    home = ParquetHome("test", path)
    assert home.path == path
