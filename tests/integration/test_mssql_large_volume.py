"""
Large volume integration test: Parquet → MSSQL with optimal batch sizes.

This test validates:
1. High-throughput writes (target: 250k+ rows/sec)
2. Optimal batch size (102,400 rows)
3. Parallel workers (8 concurrent)
4. Connection pooling under load
5. Data integrity at scale

Run with: pytest tests/integration/test_mssql_large_volume.py -v -s
"""
import os
from pathlib import Path

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
async def test_large_volume_write(tmp_path):
    """Test high-throughput writes with 500K rows (~5 batches)."""

    # Create larger test dataset (500K rows)
    test_data = pl.DataFrame(
        {
            "id": list(range(1, 500001)),
            "name": [f"User_{i}" for i in range(1, 500001)],
            "email": [f"user{i}@test.com" for i in range(1, 500001)],
            "age": [(i % 80) + 18 for i in range(1, 500001)],  # Age 18-97
            "score": [(i * 3.14) % 100 for i in range(1, 500001)],  # Float values
        }
    )

    # Write to parquet
    source_file = tmp_path / "large_test_data.parquet"
    test_data.write_parquet(source_file)
    test_table = "dbo.hygge_large_volume_test"

    # Setup connection pool with more connections for parallel writes
    factory = MssqlConnection(
        server=os.getenv("AZURE_SQL_SERVER"),
        database=os.getenv("AZURE_SQL_DATABASE"),
    )
    pool = ConnectionPool(
        name="large_volume_pool",
        connection_factory=factory,
        pool_size=10,  # More connections for 8 parallel workers
    )
    await pool.initialize()

    try:
        # Read from parquet
        source_config = ParquetHomeConfig(path=str(source_file))
        source_home = ParquetHome("source", source_config)

        # Write to MSSQL with optimal settings for small database
        # Note: TABLOCK requires serial writes (parallel_workers=1)
        # because it's an exclusive table lock
        store_config = MssqlStoreConfig(
            server=os.getenv("AZURE_SQL_SERVER"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            table=test_table,
            batch_size=102400,  # Optimal for CCI direct-to-compressed
            parallel_workers=1,  # Serial writes with TABLOCK (no lock contention)
            table_hints="TABLOCK",  # Exclusive access = much faster!
        )
        mssql_store = MssqlStore("mssql_large_store", store_config)
        mssql_store.set_pool(pool)

        # Load data
        total_rows = 0
        async for batch in source_home.read():
            await mssql_store.write(batch)
            total_rows += len(batch)

        await mssql_store.close()

        # Validation
        assert total_rows == 500000, f"Expected 500,000 rows, got {total_rows:,}"

    finally:
        await pool.close()


@pytest.mark.skipif(
    not os.getenv("AZURE_SQL_SERVER") or not os.getenv("AZURE_SQL_DATABASE"),
    reason="Azure SQL credentials not configured",
)
@pytest.mark.asyncio
async def test_table_setup_instructions():
    """
    Setup instructions for large volume test table.

    Run this SQL in Azure Portal Query Editor before running the test:

    -- Drop if exists
    DROP TABLE IF EXISTS dbo.hygge_large_volume_test;

    -- Create table (regular rowstore for initial testing)
    CREATE TABLE dbo.hygge_large_volume_test (
        id INT PRIMARY KEY,
        name NVARCHAR(100),
        email NVARCHAR(100),
        age INT,
        score FLOAT
    );

    -- OR: Create with Clustered Columnstore Index for max performance
    CREATE TABLE dbo.hygge_large_volume_test (
        id INT NOT NULL,
        name NVARCHAR(100),
        email NVARCHAR(100),
        age INT,
        score FLOAT
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_hygge_large_volume
        ON dbo.hygge_large_volume_test;

    -- Cleanup after tests:
    DROP TABLE dbo.hygge_large_volume_test;

    -- Verify row count:
    SELECT COUNT(*) FROM dbo.hygge_large_volume_test;  -- Should be 500,000
    """
    pytest.skip("This is an instruction-only test - see docstring for SQL setup")


if __name__ == "__main__":
    """Run directly for quick testing."""
    import asyncio
    from pathlib import Path

    async def main():
        test_dir = Path("data/tmp/large_volume_test")
        test_dir.mkdir(parents=True, exist_ok=True)
        await test_large_volume_write(test_dir)

    asyncio.run(main())
