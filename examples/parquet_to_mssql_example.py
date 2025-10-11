"""
Parquet to MSSQL Example

Demonstrates loading test data from parquet files into Azure SQL using hygge.

Setup:
1. Create Azure SQL database
2. Set environment variables:
   - AZURE_SQL_SERVER (e.g., yourserver.database.windows.net)
   - AZURE_SQL_DATABASE (e.g., hygge-test)
3. Create test table (SQL below)
4. Create sample parquet file or use existing test data

SQL to create test table:
```sql
CREATE TABLE dbo.test_users (
    id INT PRIMARY KEY,
    name NVARCHAR(100),
    email NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);
```

Usage:
    python examples/parquet_to_mssql_example.py
"""
import asyncio
import os
from pathlib import Path

import polars as pl

from hygge.connections import ConnectionPool, MssqlConnection
from hygge.homes.parquet import ParquetHome, ParquetHomeConfig
from hygge.stores.mssql import MssqlStore, MssqlStoreConfig


async def main():
    """Load parquet data into Azure SQL."""

    # Configuration
    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")

    if not server or not database:
        print("❌ Error: Set AZURE_SQL_SERVER and AZURE_SQL_DATABASE env vars")
        return

    # Create sample test data if it doesn't exist
    test_data_path = Path("data/test_users.parquet")
    if not test_data_path.exists():
        print(f"Creating sample test data: {test_data_path}")
        test_data_path.parent.mkdir(exist_ok=True)

        sample_data = pl.DataFrame(
            {
                "id": range(1, 101),
                "name": [f"User {i}" for i in range(1, 101)],
                "email": [f"user{i}@example.com" for i in range(1, 101)],
                "created_at": [pl.datetime(2024, 1, 1)] * 100,
            }
        )
        sample_data.write_parquet(test_data_path)
        print(f"✓ Created {len(sample_data)} sample records")

    # Setup connection pool
    print(f"Connecting to {server}/{database}...")
    factory = MssqlConnection(server=server, database=database)
    pool = ConnectionPool(
        name="azure_sql",
        connection_factory=factory,
        pool_size=10,  # 8 workers + 2 buffer
    )
    await pool.initialize()
    print("✓ Connection pool initialized")

    try:
        # Create Parquet Home (source)
        home_config = ParquetHomeConfig(path=str(test_data_path))
        home = ParquetHome("test_users", home_config)
        print(f"✓ Parquet home configured: {test_data_path}")

        # Create MSSQL Store (destination)
        store_config = MssqlStoreConfig(
            server=server,
            database=database,
            table="dbo.test_users",
            batch_size=102_400,  # Optimal for CCI
            parallel_workers=8,  # Optimal for modern SQL Server
            # table_hints="TABLOCK",  # Uncomment for exclusive access scenarios
        )
        store = MssqlStore("test_users_store", store_config)
        store.set_pool(pool)
        print("✓ MSSQL store configured: dbo.test_users")

        # Load data: parquet → MSSQL
        print("\nLoading data...")
        row_count = 0
        async for batch in home.read():
            await store.write(batch)
            row_count += len(batch)

        # Finalize
        await store.close()

        print(f"\n✓ Successfully loaded {row_count:,} rows into Azure SQL!")
        print(f"  Server: {server}")
        print(f"  Database: {database}")
        print("  Table: dbo.test_users")
        print("\nNext steps:")
        print("  1. Query the table in Azure SQL to verify data")
        print("  2. Test reading back with MssqlHome")
        print("  3. Try the full round-trip test!")

    finally:
        # Cleanup
        await pool.close()
        print("\n✓ Connection pool closed")


if __name__ == "__main__":
    asyncio.run(main())
