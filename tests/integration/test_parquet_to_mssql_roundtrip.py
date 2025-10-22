"""
Integration test for Parquet → MSSQL → Parquet round-trip.

This test demonstrates:
1. Reading test data from parquet (ParquetHome)
2. Writing to MSSQL Server (MssqlStore)
3. Reading back from MSSQL Server (MssqlHome)
4. Writing back to parquet (ParquetStore)

This validates both MSSQL home and store functionality in a real workflow.
"""
import os
from pathlib import Path

import polars as pl
import pytest

from hygge.connections import ConnectionPool, MssqlConnection
from hygge.homes.mssql import MssqlHome, MssqlHomeConfig
from hygge.homes.parquet import ParquetHome, ParquetHomeConfig
from hygge.stores.mssql import MssqlStore, MssqlStoreConfig
from hygge.stores.parquet import ParquetStore, ParquetStoreConfig


@pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER") or not os.getenv("AZURE_SQL_DATABASE"),
    reason="Azure SQL credentials not configured",
)
@pytest.mark.asyncio
async def test_parquet_to_mssql_roundtrip(tmp_path):
    """Test full round-trip: parquet → mssql → parquet."""

    # Setup: Create test data
    test_data = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "email": [
                "alice@test.com",
                "bob@test.com",
                "charlie@test.com",
                "diana@test.com",
                "eve@test.com",
            ],
        }
    )

    # Write test data to parquet
    source_parquet = tmp_path / "source.parquet"
    test_data.write_parquet(source_parquet)

    # Create connection pool for MSSQL
    factory = MssqlConnection(
        server=os.getenv("AZURE_SQL_SERVER"),
        database=os.getenv("AZURE_SQL_DATABASE"),
    )
    pool = ConnectionPool(
        name="test_pool",
        connection_factory=factory,
        pool_size=5,
    )
    await pool.initialize()

    try:
        # Step 1: Read from parquet using ParquetHome
        source_config = ParquetHomeConfig(path=str(source_parquet))
        source_home = ParquetHome("source", source_config)

        # Step 2: Write to MSSQL using MssqlStore
        # Note: Table must exist in Azure SQL (see test_table_setup below)
        test_table = "dbo.hygge_test_roundtrip"
        store_config = MssqlStoreConfig(
            server=os.getenv("AZURE_SQL_SERVER"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            table=test_table,
            batch_size=1000,  # Small batch for test
            parallel_workers=2,  # Fewer workers for test
            if_exists="replace",  # Replace existing table for test runs
        )
        mssql_store = MssqlStore("test_store", store_config)
        mssql_store.set_pool(pool)

        # Load data into MSSQL
        async for batch in source_home.read():
            await mssql_store.write(batch)
        await mssql_store.close()

        # Step 3: Read back from MSSQL using MssqlHome
        home_config = MssqlHomeConfig(
            server=os.getenv("AZURE_SQL_SERVER"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            table=test_table,
            batch_size=1000,
        )
        mssql_home = MssqlHome("test_home", home_config, pool=pool)

        # Step 4: Write back to parquet using ParquetStore
        dest_parquet = tmp_path / "dest"
        dest_config = ParquetStoreConfig(path=str(dest_parquet))
        dest_store = ParquetStore("dest", dest_config)

        async for batch in mssql_home.read():
            await dest_store.write(batch)
        await dest_store.close()

        # Verify: Read final parquet and compare
        result_files = list(Path(dest_parquet).glob("*.parquet"))
        assert len(result_files) > 0, "No parquet files written"

        result_data = pl.read_parquet(result_files[0])

        # Compare data (may need to sort for consistent comparison)
        result_data = result_data.sort("id")
        test_data = test_data.sort("id")

        assert result_data.shape == test_data.shape, "Shape mismatch"
        assert result_data["id"].to_list() == test_data["id"].to_list()
        assert result_data["name"].to_list() == test_data["name"].to_list()
        assert result_data["email"].to_list() == test_data["email"].to_list()

        print("✓ Round-trip test passed!")
        print(f"  - Loaded {len(test_data)} rows into MSSQL")
        print(f"  - Read back {len(result_data)} rows from MSSQL")
        print("  - Data integrity verified")

    finally:
        # Cleanup: Close connection pool
        await pool.close()


@pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER") or not os.getenv("AZURE_SQL_DATABASE"),
    reason="Azure SQL credentials not configured",
)
@pytest.mark.asyncio
async def test_table_setup_instructions():
    """
    Provide instructions for setting up the test table.

    Run this SQL in Azure SQL before running the round-trip test:

    CREATE TABLE dbo.hygge_test_roundtrip (
        id INT PRIMARY KEY,
        name NVARCHAR(100),
        email NVARCHAR(100)
    );

    To cleanup after tests:
    DROP TABLE dbo.hygge_test_roundtrip;
    """
    pytest.skip("This is an instruction-only test - see docstring for SQL setup")
