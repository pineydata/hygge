"""
Example of using the Coordinator to orchestrate multiple flows.
"""
import asyncio
from pathlib import Path

import polars as pl

from hygge import Coordinator


async def setup_test_data(path: str, rows: int = 1_000_000) -> None:
    """Create test parquet file."""
    df = pl.DataFrame(
        {
            "id": range(rows),
            "value": [f"test_{i}" for i in range(rows)],
            "number": [i * 6 for i in range(rows)],
        }
    )
    df.write_parquet(path)


async def main():
    # Setup paths
    base_dir = Path("data")
    base_dir.mkdir(exist_ok=True)

    # Home structure: data/home/table/
    home_dir = base_dir / "home" / "numbers"
    home_dir.mkdir(parents=True, exist_ok=True)
    source_file = home_dir / "01.parquet"

    # Store structure: data/store/
    store_dir = base_dir / "store"

    # Clean up existing store data
    if store_dir.exists():
        import shutil

        shutil.rmtree(store_dir)
        print(f"Cleaned up existing store directory: {store_dir}")

    store_dir.mkdir(exist_ok=True)

    # Create test data
    await setup_test_data(source_file)
    print(f"Created test data at {source_file}")

    # Create YAML configuration
    config_content = f"""
flows:
  numbers_flow:
    home:
      type: parquet
      path: {source_file}
      options:
        batch_size: 1000
    store:
      type: parquet
      path: {store_dir}
      options:
        batch_size: 100000
        file_pattern: "{{sequence:020d}}.parquet"
        compression: snappy
    options:
      queue_size: 5
"""

    # Write config file
    config_path = base_dir / "coordinator_config.yaml"
    with open(config_path, "w") as f:
        f.write(config_content)

    print(f"Created configuration at {config_path}")

    # Create and run coordinator
    coordinator = Coordinator(
        config_path=str(config_path),
        options={
            "max_concurrent": 1,  # Run one flow at a time for this example
            "continue_on_error": False,
        },
    )

    print("Setting up coordinator...")
    await coordinator.setup()

    print("Starting coordinator...")
    await coordinator.start()

    print("Coordinator completed successfully!")

    # Verify results
    result_files = list((store_dir / "numbers_flow").glob("*.parquet"))
    print(f"\nResults in {store_dir / 'numbers_flow'}:")
    total_rows = 0
    for file in result_files:
        df = pl.read_parquet(file)
        rows = len(df)
        total_rows += rows
        print(f"- {file.name}: {rows:,} rows")
    print(f"\nTotal rows: {total_rows:,}")


if __name__ == "__main__":
    asyncio.run(main())
