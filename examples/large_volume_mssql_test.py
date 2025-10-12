"""
Large Volume Test: Write 500K rows to Azure SQL Server.

This script validates high-throughput performance:
- 500,000 test rows (~5 batches of 102,400 rows)
- Optimal batch size: 102,400 rows
- Parallel workers: 8
- Target: 250,000+ rows/sec

Setup:
1. Ensure Azure SQL table exists (see SQL below)
2. Set environment variables
3. Run: python examples/large_volume_mssql_test.py

Table SQL:
    DROP TABLE IF EXISTS dbo.hygge_large_volume_test;

    CREATE TABLE dbo.hygge_large_volume_test (
        id INT NOT NULL,
        name NVARCHAR(100),
        email NVARCHAR(100),
        age INT,
        score FLOAT
    );

    -- Optional: Add CCI for max performance
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_hygge_large_volume
        ON dbo.hygge_large_volume_test;
"""
import asyncio
import os
import time
from pathlib import Path

import polars as pl

from hygge.connections import ConnectionPool, MssqlConnection
from hygge.homes.parquet import ParquetHome, ParquetHomeConfig
from hygge.stores.mssql import MssqlStore, MssqlStoreConfig


async def main():
    """Run large volume test."""

    # Check environment
    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")

    if not server or not database:
        print("❌ Missing environment variables:")
        print("   export AZURE_SQL_SERVER='your-server.database.windows.net'")
        print("   export AZURE_SQL_DATABASE='hygge-test'")
        return

    print("🏠 hygge Large Volume Test")
    print(f"   Server: {server}")
    print(f"   Database: {database}")
    print("   Target: 500,000 rows (~5 batches)")
    print()

    # Generate test data
    print("📊 Generating 500,000 test rows...")
    start_gen = time.time()
    test_data = pl.DataFrame(
        {
            "id": list(range(1, 500001)),
            "name": [f"User_{i}" for i in range(1, 500001)],
            "email": [f"user{i}@test.com" for i in range(1, 500001)],
            "age": [(i % 80) + 18 for i in range(1, 500001)],
            "score": [(i * 3.14) % 100 for i in range(1, 500001)],
        }
    )
    gen_time = time.time() - start_gen

    # Write to temporary parquet file
    temp_dir = Path("data/tmp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    source_file = temp_dir / "large_volume_test.parquet"
    test_data.write_parquet(source_file)
    file_size_mb = source_file.stat().st_size / (1024 * 1024)

    print(f"✓ Generated: {len(test_data):,} rows in {gen_time:.2f}s")
    print(f"✓ Parquet file: {file_size_mb:.2f} MB")
    print()

    # Setup connection pool
    factory = MssqlConnection(server=server, database=database)
    pool = ConnectionPool(
        name="large_volume_pool",
        connection_factory=factory,
        pool_size=10,  # Enough for 8 parallel workers + overhead
    )
    await pool.initialize()
    print()

    try:
        # Read from parquet
        source_config = ParquetHomeConfig(path=str(source_file))
        source_home = ParquetHome("source", source_config)

        # Write to MSSQL with TABLOCK (serial writes)
        # Note: TABLOCK requires parallel_workers=1 (exclusive table lock)
        test_table = "dbo.hygge_large_volume_test"
        store_config = MssqlStoreConfig(
            server=server,
            database=database,
            table=test_table,
            batch_size=102400,  # Optimal for CCI direct-to-compressed
            parallel_workers=1,  # Serial writes required for TABLOCK
            table_hints="TABLOCK",  # Exclusive access = much faster!
        )
        mssql_store = MssqlStore("large_store", store_config)
        mssql_store.set_pool(pool)

        print(f"🚀 Writing to {test_table}...")
        print("   Batch size: 102,400 rows")
        print("   Parallel workers: 1 (required for TABLOCK)")
        print("   Table hints: TABLOCK (exclusive table lock)")
        print()

        # Measure total time
        start_write = time.time()
        total_rows = 0

        async for batch in source_home.read():
            await mssql_store.write(batch)
            total_rows += len(batch)

        await mssql_store.close()
        total_time = time.time() - start_write

        # Calculate overall throughput
        overall_throughput = total_rows / total_time if total_time > 0 else 0

        print()
        print("=" * 60)
        print("✅ LARGE VOLUME TEST COMPLETE!")
        print("=" * 60)
        print(f"Rows written:      {total_rows:,}")
        print(f"Data size:         {file_size_mb:.2f} MB")
        print(f"Total time:        {total_time:.2f}s")
        print(f"Overall throughput: {overall_throughput:,.0f} rows/sec")
        print("Target throughput: 250,000+ rows/sec")
        print()
        if overall_throughput >= 250000:
            print("🎉 TARGET ACHIEVED! ✅")
        elif overall_throughput >= 100000:
            print("📊 Good performance, check for optimization opportunities")
        else:
            print("⚠️  Below target - check table indexes and Azure SQL tier")
        print()
        print("Verify in Azure Portal:")
        print(f"   SELECT COUNT(*) FROM {test_table};")
        print(f"   SELECT TOP 10 * FROM {test_table} ORDER BY id;")
        print()
        print("Performance Tips:")
        print("   - Use Clustered Columnstore Index (CCI) for best throughput")
        print("   - Ensure Azure SQL has adequate DTUs/vCores")
        print("   - Consider TABLOCK hint for exclusive access scenarios")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

    finally:
        await pool.close()
        print()
        print("✓ Connections closed")


if __name__ == "__main__":
    asyncio.run(main())
