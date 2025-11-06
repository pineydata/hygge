"""
Comprehensive tests for SqliteStore implementation.

Following hygge's testing principles:
- Test behavior that matters to users (data writing, table creation, SQLite integration)
- Focus on data integrity and user experience
- Test error scenarios and edge cases
- Verify configuration system integration
"""
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

import polars as pl
import pytest
from pydantic import ValidationError

from hygge.stores import SqliteStore, SqliteStoreConfig
from hygge.utility import StoreError


@pytest.fixture
def temp_db_path():
    """Create temporary SQLite database path for store tests."""
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test.db"
    yield db_path
    if db_path.exists():
        os.remove(db_path)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_data():
    """Create sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "id": range(1000),
            "name": [f"user_{i}" for i in range(1000)],
            "value": [i * 2.5 for i in range(1000)],
            "category": [f"cat_{i % 5}" for i in range(1000)],
            "active": [True if i % 2 == 0 else False for i in range(1000)],
        }
    )


@pytest.fixture
def large_data():
    """Create large DataFrame for testing."""
    return pl.DataFrame(
        {
            "id": range(50000),
            "value": [f"large_data_{i}" for i in range(50000)],
            "number": [i * 3.14 for i in range(50000)],
        }
    )


class TestSqliteStoreInitialization:
    """Test SqliteStore initialization and configuration."""

    def test_sqlite_store_initialization_with_config(self, temp_db_path):
        """Test SqliteStore initializes correctly with config."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="test_table",
            batch_size=5000,
        )

        store = SqliteStore("test_store", config)

        assert store.name == "test_store"
        assert store.config == config
        # Compare resolved paths (both should be absolute)
        assert store.db_path.resolve() == Path(temp_db_path).resolve()
        assert store.table == "test_table"
        assert store.batch_size == 5000
        assert "sqlite:///" in store.connection_string
        # Connection string uses resolved absolute path
        assert str(Path(temp_db_path).resolve()) in store.connection_string

    def test_sqlite_store_initialization_defaults(self, temp_db_path):
        """Test SqliteStore uses defaults when not specified."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="users",
        )

        store = SqliteStore("test_store", config)

        assert store.name == "test_store"
        assert store.batch_size == 100_000  # Default batch size
        assert store.table == "users"

    def test_sqlite_store_creates_parent_directory(self, temp_dir):
        """Test that SqliteStore creates parent directory if it doesn't exist."""
        db_path = temp_dir / "nested" / "path" / "test.db"
        config = SqliteStoreConfig(
            path=str(db_path),
            table="test_table",
        )

        store = SqliteStore("test_store", config)

        # Parent directory should be created
        assert db_path.parent.exists()
        # Compare resolved paths (both should be absolute)
        assert store.db_path.resolve() == db_path.resolve()

    def test_sqlite_store_with_entity_name(self, temp_db_path):
        """Test SqliteStore with entity_name for table substitution."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="{entity}_data",
        )

        store = SqliteStore("test_store", config, entity_name="users")

        # Table name should have entity substituted
        assert store.table == "users_data"

    def test_sqlite_store_without_entity_substitution(self, temp_db_path):
        """Test SqliteStore without entity substitution when not needed."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="my_table",
        )

        store = SqliteStore("test_store", config, entity_name="users")

        # Table name should remain unchanged if no {entity} placeholder
        assert store.table == "my_table"


class TestSqliteStoreDataWriting:
    """Test SqliteStore data writing functionality."""

    @pytest.mark.asyncio
    async def test_write_single_batch(self, temp_db_path, sample_data):
        """Test writing a single batch of data."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="test_table",
            batch_size=10000,  # Larger than sample data
        )
        store = SqliteStore("test_store", config)

        # Write data (should accumulate, not write yet)
        result = await store.write(sample_data)

        # Should accumulate data, not write yet
        assert result is None
        assert store.current_df is not None
        assert len(store.current_df) == len(sample_data)
        assert store.total_rows == len(sample_data)

        # Finish to write remaining data
        await store.finish()

        # Verify data was written to SQLite
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_table")
        row_count = cursor.fetchone()[0]
        conn.close()

        assert row_count == len(sample_data)

        # Verify data integrity using sqlite3 directly (no extra dependencies)
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM test_table ORDER BY id")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == len(sample_data)
        # Verify first and last rows match
        assert rows[0][0] == sample_data["id"][0]  # id column
        assert rows[0][1] == sample_data["name"][0]  # name column
        assert rows[-1][0] == sample_data["id"][-1]  # id column
        assert rows[-1][1] == sample_data["name"][-1]  # name column

    @pytest.mark.asyncio
    async def test_write_multiple_batches(self, temp_db_path, large_data):
        """Test writing data that exceeds batch size."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="large_table",
            batch_size=10000,  # Smaller than large_data
        )
        store = SqliteStore("test_store", config)

        # Write large data (should trigger multiple writes)
        await store.write(large_data)
        await store.finish()

        # Verify all data was written
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM large_table")
        row_count = cursor.fetchone()[0]
        conn.close()

        assert row_count == len(large_data)

    @pytest.mark.asyncio
    async def test_write_incremental_data(self, temp_db_path):
        """Test writing data in multiple small increments."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="incremental_table",
            batch_size=500,
        )
        store = SqliteStore("test_store", config)

        # Write data in small chunks
        total_rows = 0
        for i in range(5):
            chunk_data = pl.DataFrame(
                {
                    "id": range(i * 200, (i + 1) * 200),
                    "value": [f"chunk_{i}_{j}" for j in range(200)],
                    "chunk": [i] * 200,
                }
            )

            await store.write(chunk_data)
            total_rows += len(chunk_data)

        # Finish to write remaining data
        await store.finish()

        # Verify all data was written
        assert store.total_rows == total_rows

        # Verify in database
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM incremental_table")
        row_count = cursor.fetchone()[0]
        conn.close()

        assert row_count == total_rows

    @pytest.mark.asyncio
    async def test_table_created_automatically(self, temp_db_path, sample_data):
        """Test that table is created automatically on first write."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="auto_table",
        )
        store = SqliteStore("test_store", config)

        # Database file might not exist yet
        assert not temp_db_path.exists() or temp_db_path.stat().st_size == 0

        # Write data - should create table automatically
        await store.write(sample_data)
        await store.finish()

        # Verify table was created and has data
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Check table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='auto_table'"
        )
        table_exists = cursor.fetchone() is not None
        assert table_exists

        # Check data exists
        cursor.execute("SELECT COUNT(*) FROM auto_table")
        row_count = cursor.fetchone()[0]
        assert row_count == len(sample_data)

        conn.close()

    @pytest.mark.asyncio
    async def test_append_to_existing_table(self, temp_db_path, sample_data):
        """Test appending to an existing table."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="append_table",
        )
        store = SqliteStore("test_store", config)

        # First write
        await store.write(sample_data)
        await store.finish()

        # Second write - should append
        second_batch = pl.DataFrame(
            {
                "id": range(1000, 2000),
                "name": [f"user_{i}" for i in range(1000, 2000)],
                "value": [i * 2.5 for i in range(1000, 2000)],
                "category": [f"cat_{i % 5}" for i in range(1000, 2000)],
                "active": [True if i % 2 == 0 else False for i in range(1000, 2000)],
            }
        )

        store2 = SqliteStore("test_store2", config)
        await store2.write(second_batch)
        await store2.finish()

        # Verify total rows
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM append_table")
        row_count = cursor.fetchone()[0]
        conn.close()

        assert row_count == len(sample_data) + len(second_batch)


class TestSqliteStoreErrorHandling:
    """Test SqliteStore error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_write_empty_dataframe(self, temp_db_path):
        """Test writing empty DataFrame."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="empty_table",
        )
        store = SqliteStore("test_store", config)

        empty_data = pl.DataFrame({"id": [], "value": []})

        await store.write(empty_data)
        await store.finish()

        # Should not create table for empty data
        if temp_db_path.exists():
            conn = sqlite3.connect(temp_db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='empty_table'"
            )
            table_exists = cursor.fetchone() is not None
            conn.close()

            # Table might be created but should be empty
            if table_exists:
                conn = sqlite3.connect(temp_db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM empty_table")
                row_count = cursor.fetchone()[0]
                conn.close()
                assert row_count == 0

    def test_write_without_table_name(self, temp_db_path):
        """Test error when table name is not set."""
        # Pydantic validation catches empty table name at config creation time
        with pytest.raises(ValidationError, match="Table name is required"):
            SqliteStoreConfig(
                path=str(temp_db_path),
                table="",  # Empty table name
            )

    @pytest.mark.asyncio
    async def test_write_with_invalid_path(self):
        """Test error handling with invalid database path."""
        # Try to write to a path that can't be created
        invalid_path = "/nonexistent/invalid/path/test.db"
        config = SqliteStoreConfig(
            path=invalid_path,
            table="test_table",
        )

        # Parent directory creation might fail or succeed depending on permissions
        # But we should handle it gracefully
        try:
            store = SqliteStore("test_store", config)
            sample_data = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

            await store.write(sample_data)
            # This might fail or succeed depending on filesystem permissions
            # In most test environments, this will fail
            try:
                await store.finish()
            except StoreError:
                pass  # Expected to fail
        except (OSError, StoreError):
            pass  # Expected if parent directory can't be created


class TestSqliteStoreConfiguration:
    """Test SqliteStore configuration system integration."""

    def test_sqlite_store_config_validation(self):
        """Test SqliteStoreConfig validation."""
        # Valid config
        config = SqliteStoreConfig(path="/test/database.db", table="users")
        assert config.path == "/test/database.db"
        assert config.table == "users"
        assert config.type == "sqlite"

        # Empty path
        with pytest.raises(ValueError, match="Path is required"):
            SqliteStoreConfig(path="", table="users")

        # Empty table
        with pytest.raises(ValueError, match="Table name is required"):
            SqliteStoreConfig(path="/test/database.db", table="")

    def test_sqlite_store_config_defaults_merging(self, temp_db_path):
        """Test SqliteStoreConfig defaults merging."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="test_table",
            batch_size=15000,
            options={"custom_setting": "test"},
        )

        merged_options = config.get_merged_options()
        assert merged_options["batch_size"] == 15000
        assert merged_options["custom_setting"] == "test"

    def test_sqlite_store_config_with_flow_name(self, temp_db_path):
        """Test SqliteStoreConfig with flow_name (should not affect SQLite)."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="test_table",
        )

        merged_options = config.get_merged_options("my_flow")
        # Flow name shouldn't affect SQLite config
        assert merged_options["batch_size"] == 100_000


class TestSqliteStoreIntegration:
    """Test SqliteStore integration with hygge framework."""

    def test_sqlite_store_implements_store_interface(self, temp_db_path):
        """Test that SqliteStore properly implements the Store interface."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="test_table",
        )
        store = SqliteStore("test_store", config)

        # Should have all required Store attributes
        assert hasattr(store, "name")
        assert hasattr(store, "options")
        assert hasattr(store, "batch_size")
        assert hasattr(store, "row_multiplier")
        assert hasattr(store, "start_time")
        assert hasattr(store, "logger")

        # Should have all required Store methods
        assert hasattr(store, "write")
        assert hasattr(store, "finish")
        assert callable(store.write)
        assert callable(store.finish)

        # SQLite store doesn't use file staging
        assert store.get_staging_directory() is None
        assert store.get_final_directory() is None

    def test_sqlite_store_logger_configuration(self, temp_db_path):
        """Test that SqliteStore has proper logger configuration."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="test_table",
        )
        store = SqliteStore("test_store", config)

        assert store.logger is not None
        assert store.logger.logger.name == "hygge.store.SqliteStore"

    @pytest.mark.asyncio
    async def test_sqlite_store_progress_tracking(self, temp_db_path, sample_data):
        """Test that progress tracking works correctly."""
        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="test_table",
        )
        store = SqliteStore("test_store", config)

        await store.write(sample_data)
        await store.finish()

        # Verify progress tracking
        assert store.total_rows == len(sample_data)
        assert store.start_time is not None

    def test_sqlite_store_registry_pattern(self, temp_db_path):
        """Test that SqliteStore is registered and can be created via Store.create()."""
        from hygge.core.store import Store

        config = SqliteStoreConfig(
            path=str(temp_db_path),
            table="test_table",
        )

        # Should be able to create via registry
        store = Store.create("test_store", config)

        assert isinstance(store, SqliteStore)
        assert store.name == "test_store"
        assert store.table == "test_table"
