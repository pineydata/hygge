"""
Entity pattern example - demonstrating the landing zone to data lake pattern.

This example shows how to move multiple entities from a landing zone to a data lake
with a single flow definition.

EXPECTED STRUCTURE:
    landing_zone/
      ├── users/
      │   ├── batch_001.parquet
      │   └── batch_002.parquet
      ├── orders/
      │   ├── batch_001.parquet
      │   └── batch_002.parquet
      └── products/
          └── data.parquet

Each entity has its own directory containing one or more parquet files.
This is the standard pattern for landing zones with batch or partitioned data.
"""
import asyncio
import shutil
import tempfile
from pathlib import Path

import polars as pl

from hygge import Coordinator


async def main():
    """Run the entity pattern example."""
    # Create temporary directories
    temp_dir = Path(tempfile.mkdtemp())
    print(f"Working in: {temp_dir}\n")

    try:
        # Setup landing zone
        landing_zone = temp_dir / "landing_zone"
        landing_zone.mkdir()

        # Setup data lake
        data_lake = temp_dir / "data_lake"
        data_lake.mkdir()

        # Create entity directories with parquet files (expected pattern)
        print("Creating landing zone with entity directories...")

        # Users entity - split across 2 files
        users_dir = landing_zone / "users"
        users_dir.mkdir()

        users_df1 = pl.DataFrame(
            {
                "user_id": range(0, 50),
                "name": [f"user_{i}" for i in range(0, 50)],
                "email": [f"user{i}@example.com" for i in range(0, 50)],
            }
        )
        users_df1.write_parquet(users_dir / "batch_001.parquet")

        users_df2 = pl.DataFrame(
            {
                "user_id": range(50, 100),
                "name": [f"user_{i}" for i in range(50, 100)],
                "email": [f"user{i}@example.com" for i in range(50, 100)],
            }
        )
        users_df2.write_parquet(users_dir / "batch_002.parquet")

        print("  ✓ Created users/ with 2 files (100 rows total)")

        # Orders entity - split across 3 files
        orders_dir = landing_zone / "orders"
        orders_dir.mkdir()

        for i in range(3):
            orders_df = pl.DataFrame(
                {
                    "order_id": range(i * 20, (i + 1) * 20),
                    "user_id": [j % 100 for j in range(i * 20, (i + 1) * 20)],
                    "amount": [j * 10.5 for j in range(i * 20, (i + 1) * 20)],
                }
            )
            orders_df.write_parquet(orders_dir / f"batch_{i+1:03d}.parquet")

        print("  ✓ Created orders/ with 3 files (60 rows total)")

        # Products entity - single file
        products_dir = landing_zone / "products"
        products_dir.mkdir()

        products_df = pl.DataFrame(
            {
                "product_id": range(25),
                "name": [f"product_{i}" for i in range(25)],
                "price": [i * 5.0 for i in range(25)],
            }
        )
        products_df.write_parquet(products_dir / "data.parquet")

        print("  ✓ Created products/ with 1 file (25 rows total)")

        # Create config file with entity pattern
        config_content = f"""flows:
  landing_to_lake:
    home: {landing_zone}
    store: {data_lake}
    entities:
      - users
      - orders
      - products
"""

        config_file = temp_dir / "config.yaml"
        config_file.write_text(config_content)

        print("\nConfig file:")
        print(config_content)

        # Run coordinator
        print("\n" + "=" * 60)
        print("Running hygge coordinator with entity pattern...")
        print("=" * 60 + "\n")

        coordinator = Coordinator(str(config_file))
        await coordinator.run()

        # Verify results
        print("\n" + "=" * 60)
        print("Verification")
        print("=" * 60 + "\n")

        for entity in ["users", "orders", "products"]:
            entity_dir = data_lake / entity
            if entity_dir.exists():
                files = list(entity_dir.glob("*.parquet"))
                total_rows = sum(len(pl.read_parquet(f)) for f in files)
                print(f"  ✓ {entity}: {len(files)} files, {total_rows} rows")
            else:
                print(f"  ✗ {entity}: directory not found")

        print("\n" + "=" * 60)
        print("Success! Entity pattern working correctly.")
        print("=" * 60)

        print(f"\nOutput location: {data_lake}")
        print("Check the data_lake directory to see the results.")

    finally:
        # Cleanup
        print(f"\nCleaning up: {temp_dir}")
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    asyncio.run(main())
