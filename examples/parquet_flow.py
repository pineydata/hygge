"""
Example of Parquet to Parquet flow.
"""
import asyncio
from pathlib import Path

import polars as pl

from hygge import Flow, ParquetHome, ParquetStore


async def setup_test_data(path: str, rows: int = 10_000_000) -> None:
    """Create test parquet file."""
    df = pl.DataFrame({
        'id': range(rows),
        'value': [f'test_{i}' for i in range(rows)],
        'number': [i*6 for i in range(rows)],
    })
    df.write_parquet(path)


async def main():
    # Setup paths
    base_dir = Path('data')
    base_dir.mkdir(exist_ok=True)

    # Home structure: data/home/table/
    home_dir = base_dir / 'home' / 'numbers'
    home_dir.mkdir(parents=True, exist_ok=True)
    source_file = home_dir / '01.parquet'

    # Store structure: data/store/
    store_dir = base_dir / 'store'

    # Clean up existing store data
    if store_dir.exists():
        import shutil
        shutil.rmtree(store_dir)
        print(f"Cleaned up existing store directory: {store_dir}")

    store_dir.mkdir(exist_ok=True)

    # Create test data
    await setup_test_data(source_file)
    print(f"Created test data at {source_file}")

    # Create and run flow - Flow will instantiate Home and Store
    flow = Flow(
        name="numbers_flow",
        home_class=ParquetHome,
        home_config={
            'name': 'numbers',
            'path': str(source_file),
            'options': {
                'batch_size': 100  # Small batch for testing
            }
        },
        store_class=ParquetStore,
        store_config={
            'name': 'numbers',
            'path': str(store_dir),
            'options': {
                'batch_size': 500000,  # Different batch size to test accumulation
                'file_pattern': "{sequence:020d}.parquet",
                'compression': 'snappy'
            }
        },
        options={
            'queue_size': 5  # Small queue for testing
        }
    )

    print("Starting flow...")
    await flow.start()
    print("Flow completed")

    # Verify results
    result_files = list((store_dir / 'numbers').glob('*.parquet'))
    print(f"\nResults in {store_dir / 'numbers'}:")
    total_rows = 0
    for file in result_files:
        df = pl.read_parquet(file)
        rows = len(df)
        total_rows += rows
        print(f"- {file.name}: {rows:,} rows")
    print(f"\nTotal rows: {total_rows:,}")


if __name__ == '__main__':
    asyncio.run(main())
