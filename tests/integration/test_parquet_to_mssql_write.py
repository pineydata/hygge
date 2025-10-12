"""
Simple integration test: Parquet → MSSQL (write only).

This test validates MssqlStore can write data to Azure SQL Server.
"""
import os

import polars as pl
import pytest

from hygge.connections import ConnectionPool, MssqlConnection
from hygge.homes.parquet import ParquetHome, ParquetHomeConfig
from hygge.stores.mssql import MssqlStore, MssqlStoreConfig


@pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER") or not os.getenv("AZURE_SQL_DATABASE"),
    reason="Azure SQL credentials not configured",
)
@pytest.mark.asyncio
async def test_parquet_to_mssql_write(tmp_path):
    """Test writing parquet data to MSSQL Server."""

    # Create test data
    test_data = pl.DataFrame(
        {
            "id": list(range(1, 101)),  # 100 rows
            "name": [f"User_{i}" for i in range(1, 101)],
            "email": [f"user{i}@test.com" for i in range(1, 101)],
        }
    )

    # Write to parquet
    source_file = tmp_path / "test_data.parquet"
    test_data.write_parquet(source_file)

    print(f"\n✓ Created test data: {len(test_data)} rows")

    # Setup connection pool
    factory = MssqlConnection(
        server=os.getenv("AZURE_SQL_SERVER"),
        database=os.getenv("AZURE_SQL_DATABASE"),
    )
    pool = ConnectionPool(
        name="test_pool",
        connection_factory=factory,
        pool_size=3,
    )
    await pool.initialize()

    try:
        # Read from parquet
        source_config = ParquetHomeConfig(path=str(source_file))
        source_home = ParquetHome("source", source_config)

        # Write to MSSQL
        test_table = "dbo.hygge_test_roundtrip"
        store_config = MssqlStoreConfig(
            server=os.getenv("AZURE_SQL_SERVER"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            table=test_table,
            batch_size=50,  # Small batches to see multiple writes
            parallel_workers=2,
        )
        mssql_store = MssqlStore("mssql_store", store_config)
        mssql_store.set_pool(pool)

        # Load data
        batch_count = 0
        total_rows = 0
        async for batch in source_home.read():
            await mssql_store.write(batch)
            batch_count += 1
            total_rows += len(batch)
            print(f"  Batch {batch_count}: {len(batch)} rows")

        await mssql_store.close()

        print(f"\n✓ Wrote {total_rows} rows in {batch_count} batches")
        print(f"✓ Table: {test_table}")
        print("\nVerify in Azure Portal:")
        print(f"  SELECT COUNT(*) FROM {test_table};")
        print(f"  SELECT TOP 10 * FROM {test_table};")

        # Basic validation - no errors means success
        assert batch_count > 0, "No batches written"
        assert total_rows == 100, f"Expected 100 rows, got {total_rows}"

    finally:
        await pool.close()


if __name__ == "__main__":
    """Run directly for quick testing."""
    import asyncio
    from pathlib import Path

    async def main():
        test_dir = Path("data/tmp/mssql_test")
        test_dir.mkdir(parents=True, exist_ok=True)
        await test_parquet_to_mssql_write(test_dir)

    asyncio.run(main())
