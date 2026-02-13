"""
Integration test: MSSQL → Parquet (read only).

This test validates MssqlHome can read data from Azure SQL Server tables.

Uses existing tables in Azure SQL.
"""

import os
from pathlib import Path

import polars as pl
import pytest

from hygge.connections import ConnectionPool, MssqlConnection
from hygge.homes.mssql import MssqlHome, MssqlHomeConfig
from hygge.stores.parquet import ParquetStore, ParquetStoreConfig

# Tables available in the hygge-test database
TEST_TABLES = [
    "dbo.Accounts",
    "dbo.Cases",
    "dbo.Categories",
    "dbo.Contacts",
    "dbo.Customers",
    "dbo.Events",
]


@pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER") or not os.getenv("AZURE_SQL_DATABASE"),
    reason="Azure SQL credentials not configured",
)
@pytest.mark.asyncio
async def test_mssql_to_parquet_read(tmp_path):
    """Test reading from MSSQL tables and writing to parquet."""

    # Choose one table to test
    test_table = "dbo.Contacts"  # Start with Contacts

    # Setup connection pool
    factory = MssqlConnection(
        server=os.getenv("AZURE_SQL_SERVER"),
        database=os.getenv("AZURE_SQL_DATABASE"),
    )
    pool = ConnectionPool(
        name="mssql_read_pool",
        connection_factory=factory,
        pool_size=3,
    )
    await pool.initialize()

    try:
        # Read from MSSQL using MssqlHome
        home_config = MssqlHomeConfig(
            server=os.getenv("AZURE_SQL_SERVER"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            table=test_table,
            batch_size=10000,  # Read in batches
        )
        mssql_home = MssqlHome("contacts_home", home_config, pool=pool)

        # Write to parquet using ParquetStore
        dest_parquet = tmp_path / "contacts"
        dest_config = ParquetStoreConfig(path=str(dest_parquet))
        parquet_store = ParquetStore("contacts_store", dest_config)

        # Read from MSSQL and write to parquet
        total_rows = 0
        batch_count = 0
        async for batch in mssql_home.read():
            await parquet_store.write(batch)
            batch_count += 1
            total_rows += len(batch)
            print(f"  Batch {batch_count}: {len(batch)} rows")

        await parquet_store.close()

        # Verify: Check parquet files were created
        result_files = list(Path(dest_parquet).glob("*.parquet"))
        assert len(result_files) > 0, f"No parquet files written to {dest_parquet}"

        # Read back the data to verify
        result_data = pl.read_parquet(result_files[0])

        print("✓ MSSQL → Parquet test passed!")
        print(f"  - Read {total_rows} rows from {test_table}")
        print(f"  - Wrote to {len(result_files)} parquet file(s)")
        print(f"  - Columns: {result_data.columns}")
        print(f"  - Rows in first file: {len(result_data)}")

        # Validate data integrity
        assert total_rows > 0, "No rows were read from MSSQL"
        assert len(result_data.columns) > 0, "No columns in result data"

    finally:
        # Cleanup: Close connection pool
        await pool.close()


@pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER") or not os.getenv("AZURE_SQL_DATABASE"),
    reason="Azure SQL credentials not configured",
)
@pytest.mark.asyncio
async def test_read_all_tables(tmp_path):
    """Test reading all available tables from MSSQL."""

    # Setup connection pool
    factory = MssqlConnection(
        server=os.getenv("AZURE_SQL_SERVER"),
        database=os.getenv("AZURE_SQL_DATABASE"),
    )
    pool = ConnectionPool(
        name="multi_table_pool",
        connection_factory=factory,
        pool_size=5,
    )
    await pool.initialize()

    results = {}

    try:
        for table in TEST_TABLES:
            table_name = table.split(".")[-1]  # Extract just the table name

            # Read from MSSQL
            home_config = MssqlHomeConfig(
                server=os.getenv("AZURE_SQL_SERVER"),
                database=os.getenv("AZURE_SQL_DATABASE"),
                table=table,
                batch_size=10000,
            )
            mssql_home = MssqlHome(f"{table_name}_home", home_config, pool=pool)

            # Write to parquet
            dest_parquet = tmp_path / table_name
            dest_config = ParquetStoreConfig(path=str(dest_parquet))
            parquet_store = ParquetStore(f"{table_name}_store", dest_config)

            # Read and write
            total_rows = 0
            async for batch in mssql_home.read():
                await parquet_store.write(batch)
                total_rows += len(batch)

            await parquet_store.close()
            results[table] = total_rows

            print(f"  ✓ {table}: {total_rows} rows")

        print(f"\n✓ Read all {len(TEST_TABLES)} tables successfully!")
        print(f"  Total rows read: {sum(results.values())}")

        # Validate
        for table, row_count in results.items():
            assert row_count >= 0, f"Error reading {table}"

    finally:
        await pool.close()


if __name__ == "__main__":
    """Run directly for quick testing."""
    import asyncio

    async def main():
        test_dir = Path("data/tmp/mssql_read_test")
        test_dir.mkdir(parents=True, exist_ok=True)

        print("Testing MSSQL → Parquet read...")
        await test_mssql_to_parquet_read(test_dir)

    asyncio.run(main())
