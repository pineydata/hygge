"""
Tests for SQLHome implementation.
"""
import pytest
import polars as pl
from datetime import datetime
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

from hygge.core.homes import SQLHome
from hygge.utils.exceptions import HomeError

class MockConnection:
    """Mock database connection for testing."""
    def __init__(self, data=None):
        self.data = data or []
        self.closed = False

    async def close(self):
        self.closed = True

@pytest.fixture
def sample_data():
    """Generate sample data for testing."""
    return [
        {"id": i, "value": f"test_{i}", "timestamp": datetime.now()}
        for i in range(1000)
    ]

@pytest.fixture
def mock_connection(sample_data):
    """Create a mock connection with sample data."""
    return MockConnection(sample_data)

@pytest.fixture
def sql_home(mock_connection):
    """Create a SQLHome instance with mock connection."""
    return SQLHome(
        "test_table",
        connection=mock_connection,
        options={
            'schema': 'test_schema',
            'table': 'test_table',
            'batch_size': 100
        }
    )

@pytest.mark.asyncio
async def test_sql_home_initialization(mock_connection):
    """Test SQLHome initialization with different options."""
    # Test with minimal options
    home = SQLHome("test", mock_connection)
    assert home.name == "test"
    assert home.connection == mock_connection
    assert home.batch_size == 10_000  # default

    # Test with custom options
    home = SQLHome(
        "test",
        mock_connection,
        options={
            'schema': 'custom_schema',
            'table': 'custom_table',
            'batch_size': 5000
        }
    )
    assert home.batch_size == 5000

    @pytest.mark.asyncio
    async def test_prepare_query(sql_home):
        """Test query preparation with default configuration."""
        query = await sql_home._prepare_query()
        assert query == "SELECT * FROM test_schema.test_table"

        # Test default schema
        sql_home = SQLHome("test_table", connection=MockConnection())
        query = await sql_home._prepare_query()
        assert query == "SELECT * FROM dbo.test_table"

@pytest.mark.asyncio
async def test_connection_cleanup(sql_home):
    """Test connection cleanup on close."""
    await sql_home.close()
    assert sql_home.connection.closed

@pytest.mark.asyncio
async def test_read_with_polars(monkeypatch):
    """Test reading data using polars."""
    # Mock polars read_database
    sample_df = pl.DataFrame({
        "id": range(10),
        "value": [f"test_{i}" for i in range(10)]
    })

    mock_read = MagicMock(return_value=[sample_df])
    monkeypatch.setattr(pl, "read_database", mock_read)

    home = SQLHome(
        "test",
        connection=MockConnection()
    )

    batches = []
    async for df in home.read():
        batches.append(df)

    assert len(batches) == 1
    assert isinstance(batches[0], pl.DataFrame)
    assert len(batches[0]) == 10

    # Verify polars was called correctly
    mock_read.assert_called_once_with(
        "SELECT * FROM dbo.test",
        home.connection,
        iter_batches=True,
        batch_size=home.batch_size
    )
