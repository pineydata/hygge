#!/usr/bin/env python3
"""
Simple Parquet-to-Parquet Example

One file that demonstrates hygge's registry pattern for parquet data movement.
Creates sample data, runs the flow, and shows results.
"""

import asyncio
import sys
from pathlib import Path

import polars as pl

# Add src to path so we can import hygge
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import parquet implementations to register them in the registry
from hygge import Coordinator, Flow, Home, HomeConfig, Store, StoreConfig
from hygge.homes.parquet.home import ParquetHome, ParquetHomeConfig  # noqa: F401
from hygge.stores.parquet.store import ParquetStore, ParquetStoreConfig  # noqa: F401


def create_sample_data():
    """Create sample parquet data for the example."""
    print("📊 Creating sample data...")

    # Create directories
    home_dir = Path("data/home/numbers")
    home_dir.mkdir(parents=True, exist_ok=True)

    # Generate sample data
    data = pl.DataFrame(
        {
            "id": range(1, 1001),
            "name": [f"user_{i:04d}" for i in range(1, 1001)],
            "value": [i * 1.5 for i in range(1, 1001)],
            "category": [f"cat_{i % 5}" for i in range(1, 1001)],
        }
    )

    # Write sample parquet file
    sample_file = home_dir / "01.parquet"
    data.write_parquet(sample_file)

    print(f"✅ Created {len(data):,} rows in {sample_file}")
    return sample_file


async def run_yaml_example():
    """Run example using YAML configuration."""
    print("\n🔄 Running YAML Configuration Example")
    print("=" * 50)

    # YAML configuration as string
    yaml_config = """
flows:
  parquet_demo:
    home:
      type: parquet
      path: data/home/numbers/01.parquet
    store:
      type: parquet
      path: data/output/numbers
    queue_size: 5

options:
  log_level: INFO
"""

    # Write YAML config to file
    config_path = Path("data/output/example_config.yaml")
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml_config.strip())

    print("📝 YAML Config:")
    print(yaml_config)

    # Run with Coordinator
    coordinator = Coordinator(str(config_path))
    await coordinator.run()

    print("✅ YAML example completed!")


async def run_programmatic_example():
    """Run example using programmatic configuration."""
    print("\n🔄 Running Programmatic Example")
    print("=" * 50)

    # Registry pattern automatically detects parquet type from paths
    home_config = HomeConfig.create("data/home/numbers/01.parquet")
    store_config = StoreConfig.create("data/output/programmatic")

    print(f"📖 Home: {home_config.path} (type: {home_config.type})")
    print(f"💾 Store: {store_config.path} (type: {store_config.type})")

    # Create actual Home and Store instances using registry pattern
    home = Home.create("programmatic_home", home_config)
    store = Store.create("programmatic_store", store_config)

    # Create and run flow
    flow = Flow("programmatic_demo", home, store)
    await flow.start()

    print("✅ Programmatic example completed!")


def show_results():
    """Show the results of the data movement."""
    print("\n📁 Results:")
    print("=" * 30)

    output_dir = Path("data/output")
    if output_dir.exists():
        for subdir in output_dir.iterdir():
            if subdir.is_dir() and subdir.name != "tmp":
                parquet_files = list(subdir.glob("*.parquet"))
                if parquet_files:
                    print(f"\n📂 {subdir.name}/")
                    for file in parquet_files:
                        size = file.stat().st_size
                        print(f"   📄 {file.name} ({size:,} bytes)")


async def main():
    """Run the complete example."""
    print("🏠 hygge Parquet-to-Parquet Example")
    print("=" * 60)
    print("Demonstrating registry pattern and simple configuration")

    # Create sample data
    create_sample_data()

    # Run YAML example
    await run_yaml_example()

    # Show results
    show_results()

    print("\n🎉 Example completed successfully!")
    print("\nKey features demonstrated:")
    print("  ✅ Registry pattern with explicit type configuration")
    print("  ✅ Simple YAML configuration")
    print("  ✅ Real parquet data movement")
    print("  ✅ Progress tracking and error handling")


if __name__ == "__main__":
    asyncio.run(main())
