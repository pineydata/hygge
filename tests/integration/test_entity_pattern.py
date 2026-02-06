"""
Integration test for entity-based flow pattern.

Tests the real-world scenario where multiple entities in a landing zone
need to flow to separate destinations.

Expected structure (entity directories):
    landing_zone/
      ├── users/
      │   ├── batch_001.parquet
      │   └── batch_002.parquet
      ├── orders/
      │   └── data.parquet
      └── products/
          └── data.parquet

Instead of defining 3 separate flows, user can define one flow with entities:

    flows:
      landing_to_lake:
        home: landing_zone/
        store: data_lake/
        entities:
          - users
          - orders
          - products

Following hygge's philosophy:
- Explicit over magic (no auto-discovery)
- DRY (don't repeat paths)
- Opinionated structure (entity directories)
"""
import shutil
import tempfile
from pathlib import Path

import polars as pl
import pytest

from hygge import Coordinator


def _path_for_yaml(p) -> str:
    """Path as YAML-safe string (forward slashes) so backslashes don't break parsing on Windows."""
    return Path(p).as_posix()


@pytest.fixture
def landing_zone_setup():
    """
    Create a realistic landing zone scenario.

    Expected pattern: home_path/{entity}/*.parquet (directories with parquet files)
    """
    workspace_tmp = Path(__file__).resolve().parent
    temp_dir = tempfile.mkdtemp(dir=str(workspace_tmp))
    temp_path = Path(temp_dir)

    # Create landing zone directory
    landing_zone = temp_path / "landing_zone"
    landing_zone.mkdir(parents=True, exist_ok=True)

    # Create data lake directory
    data_lake = temp_path / "data_lake"
    data_lake.mkdir(parents=True, exist_ok=True)

    # Create entity directories with parquet files (expected pattern)
    users_dir = landing_zone / "users"
    users_dir.mkdir(parents=True, exist_ok=True)

    # Split users data across multiple files (realistic!)
    users_df1 = pl.DataFrame(
        {
            "user_id": range(0, 500),
            "name": [f"user_{i}" for i in range(0, 500)],
            "email": [f"user{i}@example.com" for i in range(0, 500)],
        }
    )
    users_df1.write_parquet(users_dir / "batch_001.parquet")

    users_df2 = pl.DataFrame(
        {
            "user_id": range(500, 1000),
            "name": [f"user_{i}" for i in range(500, 1000)],
            "email": [f"user{i}@example.com" for i in range(500, 1000)],
        }
    )
    users_df2.write_parquet(users_dir / "batch_002.parquet")

    # Orders entity with multiple files
    orders_dir = landing_zone / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)

    orders_df1 = pl.DataFrame(
        {
            "order_id": range(0, 1000),
            "user_id": [i % 1000 for i in range(0, 1000)],
            "amount": [i * 10.5 for i in range(0, 1000)],
        }
    )
    orders_df1.write_parquet(orders_dir / "batch_001.parquet")

    orders_df2 = pl.DataFrame(
        {
            "order_id": range(1000, 2000),
            "user_id": [i % 1000 for i in range(1000, 2000)],
            "amount": [i * 10.5 for i in range(1000, 2000)],
        }
    )
    orders_df2.write_parquet(orders_dir / "batch_002.parquet")

    # Products entity with single file in directory
    products_dir = landing_zone / "products"
    products_dir.mkdir(parents=True, exist_ok=True)

    products_df = pl.DataFrame(
        {
            "product_id": range(500),
            "name": [f"product_{i}" for i in range(500)],
            "price": [i * 5.0 for i in range(500)],
        }
    )
    products_df.write_parquet(products_dir / "data.parquet")

    yield {
        "temp_path": temp_path,
        "landing_zone": landing_zone,
        "data_lake": data_lake,
        "expected_rows": {
            "users": 1000,
            "orders": 2000,
            "products": 500,
        },
    }

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def landing_zone_edge_case():
    """
    Single parquet files scenario (for implementation coverage).

    Tests that single files work, though directories are the expected pattern.
    """
    workspace_tmp = Path(__file__).resolve().parent
    temp_dir = tempfile.mkdtemp(dir=str(workspace_tmp))
    temp_path = Path(temp_dir)

    landing_zone = temp_path / "landing_zone"
    landing_zone.mkdir(parents=True, exist_ok=True)

    data_lake = temp_path / "data_lake"
    data_lake.mkdir(parents=True, exist_ok=True)

    # Single parquet files (not directories)
    users_df = pl.DataFrame(
        {
            "user_id": range(1000),
            "name": [f"user_{i}" for i in range(1000)],
        }
    )
    users_df.write_parquet(landing_zone / "users.parquet")

    orders_df = pl.DataFrame(
        {
            "order_id": range(500),
            "amount": [i * 10.5 for i in range(500)],
        }
    )
    orders_df.write_parquet(landing_zone / "orders.parquet")

    yield {
        "temp_path": temp_path,
        "landing_zone": landing_zone,
        "data_lake": data_lake,
        "expected_rows": {
            "users": 1000,
            "orders": 500,
        },
    }

    shutil.rmtree(temp_dir, ignore_errors=True)


class TestEntityPattern:
    """Test entity-based flow pattern for landing zone scenarios."""

    @pytest.mark.asyncio
    async def test_entity_pattern_basic(self, landing_zone_setup):
        """Test basic entity pattern with multiple entities in one flow definition."""
        setup = landing_zone_setup

        # Create workspace with entity pattern
        hygge_file = setup["temp_path"] / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        flows_dir = setup["temp_path"] / "flows"
        flows_dir.mkdir()

        landing_flow_dir = flows_dir / "landing_to_lake"
        landing_flow_dir.mkdir()
        landing_flow_file = landing_flow_dir / "flow.yml"
        landing_flow_file.write_text(
            f"""
name: "landing_to_lake"
home:
  type: "parquet"
  path: "{_path_for_yaml(setup['landing_zone'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(setup['data_lake'])}"
entities:
  - users
  - orders
  - products
"""
        )

        # Run coordinator
        coordinator = Coordinator(str(hygge_file))
        await coordinator.run()

        # Verify each entity created its own output directory
        users_output = setup["data_lake"] / "users"
        orders_output = setup["data_lake"] / "orders"
        products_output = setup["data_lake"] / "products"

        assert users_output.exists(), "Users output directory should exist"
        assert orders_output.exists(), "Orders output directory should exist"
        assert products_output.exists(), "Products output directory should exist"

        # Verify data integrity for each entity
        users_files = list(users_output.glob("*.parquet"))
        orders_files = list(orders_output.glob("*.parquet"))
        products_files = list(products_output.glob("*.parquet"))

        assert len(users_files) > 0, "Users output files should exist"
        assert len(orders_files) > 0, "Orders output files should exist"
        assert len(products_files) > 0, "Products output files should exist"

        # Verify row counts match
        total_users = sum(len(pl.read_parquet(f)) for f in users_files)
        total_orders = sum(len(pl.read_parquet(f)) for f in orders_files)
        total_products = sum(len(pl.read_parquet(f)) for f in products_files)

        assert total_users == setup["expected_rows"]["users"]
        assert total_orders == setup["expected_rows"]["orders"]
        assert total_products == setup["expected_rows"]["products"]

    @pytest.mark.asyncio
    async def test_entity_pattern_with_custom_options(self, landing_zone_setup):
        """Test entity pattern with custom flow options."""
        setup = landing_zone_setup

        # Create workspace with custom options that apply to all entities
        hygge_file = setup["temp_path"] / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        flows_dir = setup["temp_path"] / "flows"
        flows_dir.mkdir()

        landing_flow_dir = flows_dir / "landing_to_lake"
        landing_flow_dir.mkdir()
        landing_flow_file = landing_flow_dir / "flow.yml"
        landing_flow_file.write_text(
            f"""
name: "landing_to_lake"
home:
  type: "parquet"
  path: "{_path_for_yaml(setup['landing_zone'])}"
  options:
    batch_size: 500
store:
  type: "parquet"
  path: "{_path_for_yaml(setup['data_lake'])}"
  options:
    batch_size: 1000
    compression: "gzip"
entities:
  - users
  - orders
options:
  queue_size: 5
"""
        )

        coordinator = Coordinator(str(hygge_file))
        await coordinator.run()

        # Verify outputs exist
        users_output = setup["data_lake"] / "users"
        orders_output = setup["data_lake"] / "orders"

        assert users_output.exists()
        assert orders_output.exists()

        # Verify data was written
        assert len(list(users_output.glob("*.parquet"))) > 0
        assert len(list(orders_output.glob("*.parquet"))) > 0

    @pytest.mark.asyncio
    async def test_entity_pattern_parallel_execution(self, landing_zone_setup):
        """Test that entities run in parallel (existing Coordinator behavior)."""
        setup = landing_zone_setup

        hygge_file = setup["temp_path"] / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        flows_dir = setup["temp_path"] / "flows"
        flows_dir.mkdir()

        landing_flow_dir = flows_dir / "landing_to_lake"
        landing_flow_dir.mkdir()
        landing_flow_file = landing_flow_dir / "flow.yml"
        landing_flow_file.write_text(
            f"""
name: "landing_to_lake"
home:
  type: "parquet"
  path: "{_path_for_yaml(setup['landing_zone'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(setup['data_lake'])}"
entities:
  - users
  - orders
  - products
"""
        )

        coordinator = Coordinator(str(hygge_file))
        await coordinator.run()

        # With parallel execution, all 3 entities should complete successfully
        # Verify all outputs exist
        assert (setup["data_lake"] / "users").exists()
        assert (setup["data_lake"] / "orders").exists()
        assert (setup["data_lake"] / "products").exists()

    @pytest.mark.asyncio
    async def test_entity_pattern_missing_file(self, landing_zone_setup):
        """Test error handling when an entity file doesn't exist."""
        setup = landing_zone_setup

        hygge_file = setup["temp_path"] / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        flows_dir = setup["temp_path"] / "flows"
        flows_dir.mkdir()

        landing_flow_dir = flows_dir / "landing_to_lake"
        landing_flow_dir.mkdir()
        landing_flow_file = landing_flow_dir / "flow.yml"
        landing_flow_file.write_text(
            f"""
name: "landing_to_lake"
home:
  type: "parquet"
  path: "{_path_for_yaml(setup['landing_zone'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(setup['data_lake'])}"
entities:
  - users
  - nonexistent_entity
"""
        )

        coordinator = Coordinator(str(hygge_file))

        # Should raise an error for missing file
        with pytest.raises(Exception):  # Will be HomeError or FileNotFoundError
            await coordinator.run()

    @pytest.mark.asyncio
    async def test_entity_pattern_mixed_with_regular_flows(self, landing_zone_setup):
        """Test that entity pattern can coexist with regular flow definitions."""
        setup = landing_zone_setup

        # Create an additional standalone parquet file
        standalone_df = pl.DataFrame(
            {"id": range(100), "value": [f"standalone_{i}" for i in range(100)]}
        )
        standalone_file = setup["temp_path"] / "standalone.parquet"
        standalone_df.write_parquet(standalone_file)

        hygge_file = setup["temp_path"] / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        flows_dir = setup["temp_path"] / "flows"
        flows_dir.mkdir()

        # Create landing_to_lake flow with entities
        landing_flow_dir = flows_dir / "landing_to_lake"
        landing_flow_dir.mkdir()
        landing_flow_file = landing_flow_dir / "flow.yml"
        landing_flow_file.write_text(
            f"""
name: "landing_to_lake"
home:
  type: "parquet"
  path: "{_path_for_yaml(setup['landing_zone'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(setup['data_lake'])}"
entities:
  - users
  - orders
"""
        )

        # Create standalone_flow without entities
        standalone_flow_dir = flows_dir / "standalone_flow"
        standalone_flow_dir.mkdir()
        standalone_flow_file = standalone_flow_dir / "flow.yml"
        standalone_flow_file.write_text(
            f"""
name: "standalone_flow"
home:
  type: "parquet"
  path: "{_path_for_yaml(standalone_file)}"
store:
  type: "parquet"
  path: "{_path_for_yaml(setup['data_lake'] / 'standalone')}"
"""
        )

        coordinator = Coordinator(str(hygge_file))
        await coordinator.run()

        # Verify entity flows worked
        assert (setup["data_lake"] / "users").exists()
        assert (setup["data_lake"] / "orders").exists()

        # Verify regular flow worked
        assert (setup["data_lake"] / "standalone").exists()
        standalone_files = list((setup["data_lake"] / "standalone").glob("*.parquet"))
        assert len(standalone_files) > 0
        total_rows = sum(len(pl.read_parquet(f)) for f in standalone_files)
        assert total_rows == 100

    @pytest.mark.asyncio
    async def test_entity_pattern_simple_syntax(self, landing_zone_setup):
        """Test the simplest possible entity pattern syntax."""
        setup = landing_zone_setup

        hygge_file = setup["temp_path"] / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        flows_dir = setup["temp_path"] / "flows"
        flows_dir.mkdir()

        landing_flow_dir = flows_dir / "landing_to_lake"
        landing_flow_dir.mkdir()
        landing_flow_file = landing_flow_dir / "flow.yml"
        landing_flow_file.write_text(
            f"""
name: "landing_to_lake"
home:
  type: "parquet"
  path: "{_path_for_yaml(setup['landing_zone'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(setup['data_lake'])}"
entities:
  - users
"""
        )

        coordinator = Coordinator(str(hygge_file))
        await coordinator.run()

        # Should just work with minimal config
        users_output = setup["data_lake"] / "users"
        assert users_output.exists()
        users_files = list(users_output.glob("*.parquet"))
        assert len(users_files) > 0

        total_rows = sum(len(pl.read_parquet(f)) for f in users_files)
        assert total_rows == setup["expected_rows"]["users"]

    @pytest.mark.asyncio
    async def test_entity_pattern_edge_case_single_files(self, landing_zone_edge_case):
        """
        Test implementation coverage: single parquet files instead of directories.

        This works but isn't the documented pattern. Kept for implementation coverage.
        """
        setup = landing_zone_edge_case

        hygge_file = setup["temp_path"] / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        flows_dir = setup["temp_path"] / "flows"
        flows_dir.mkdir()

        landing_flow_dir = flows_dir / "landing_to_lake"
        landing_flow_dir.mkdir()
        landing_flow_file = landing_flow_dir / "flow.yml"
        landing_flow_file.write_text(
            f"""
name: "landing_to_lake"
home:
  type: "parquet"
  path: "{_path_for_yaml(setup['landing_zone'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(setup['data_lake'])}"
entities:
  - users.parquet
  - orders.parquet
"""
        )

        coordinator = Coordinator(str(hygge_file))
        await coordinator.run()

        # Verify output - each entity creates its own directory
        # users.parquet → data_lake/users.parquet/
        # orders.parquet → data_lake/orders.parquet/

        users_output = setup["data_lake"] / "users.parquet"
        orders_output = setup["data_lake"] / "orders.parquet"

        assert users_output.exists(), "Users output directory should exist"
        assert orders_output.exists(), "Orders output directory should exist"

        # Count rows in final locations only (exclude tmp directory)
        users_files = list(users_output.glob("*.parquet"))
        orders_files = list(orders_output.glob("*.parquet"))

        total_users = sum(len(pl.read_parquet(f)) for f in users_files)
        total_orders = sum(len(pl.read_parquet(f)) for f in orders_files)

        assert total_users == setup["expected_rows"]["users"]
        assert total_orders == setup["expected_rows"]["orders"]
