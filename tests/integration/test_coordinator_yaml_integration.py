"""
Integration test for Coordinator with YAML configuration.

Tests the complete Coordinator workflow:
1. Read YAML configuration file
2. Validate configuration with Pydantic
3. Create flows using Factory
4. Orchestrate multiple flows in parallel
5. Handle errors and completion

Following hygge's testing philosophy:
- Test real YAML configurations that users would write
- Verify end-to-end orchestration works
- Focus on user experience and configuration validation
"""
import asyncio
import shutil
import tempfile
from pathlib import Path
from typing import Dict

import polars as pl
import pytest

from hygge import Coordinator

# Import concrete implementations to register them
from hygge.utility.exceptions import ConfigError, HomeReadError


def _path_for_yaml(p) -> str:
    """Path as YAML-safe string (forward slashes) so backslashes don't break parsing on Windows."""
    return Path(p).as_posix()


@pytest.fixture
def temp_config_dir():
    """Create temporary directory for test configurations (inside workspace for sandbox)."""
    workspace_tmp = Path(__file__).resolve().parent
    temp_dir = tempfile.mkdtemp(dir=str(workspace_tmp))
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_data_files(temp_config_dir: Path) -> Dict[str, Path]:
    """Create sample data files for testing."""
    data_dir = temp_config_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Create multiple test files
    files = {}

    # Users data
    users_df = pl.DataFrame(
        {
            "user_id": range(1000),
            "name": [f"user_{i}" for i in range(1000)],
            "email": [f"user{i}@example.com" for i in range(1000)],
            "active": [True] * 1000,
        }
    )
    users_file = data_dir / "users.parquet"
    users_df.write_parquet(users_file)
    files["users"] = users_file

    # Orders data
    orders_df = pl.DataFrame(
        {
            "order_id": range(2000),
            "user_id": [i % 1000 for i in range(2000)],
            "amount": [i * 10.5 for i in range(2000)],
            "status": [
                ["pending", "completed", "cancelled"][i % 3] for i in range(2000)
            ],
        }
    )
    orders_file = data_dir / "orders.parquet"
    orders_df.write_parquet(orders_file)
    files["orders"] = orders_file

    # Products data
    products_df = pl.DataFrame(
        {
            "product_id": range(500),
            "name": [f"product_{i}" for i in range(500)],
            "price": [i * 5.0 for i in range(500)],
            "category": [f"cat_{i % 10}" for i in range(500)],
        }
    )
    products_file = data_dir / "products.parquet"
    products_df.write_parquet(products_file)
    files["products"] = products_file

    return files


@pytest.fixture
def simple_config_file(
    temp_config_dir: Path, sample_data_files: Dict[str, Path]
) -> Path:
    """Create simple workspace configuration (hygge.yml + flows/)."""
    # Create hygge.yml
    hygge_file = temp_config_dir / "hygge.yml"
    hygge_file.write_text(
        """
name: "test_project"
flows_dir: "flows"
"""
    )

    # Create flows directory
    flows_dir = temp_config_dir / "flows"
    flows_dir.mkdir()

    # Create users_flow
    users_flow_dir = flows_dir / "users_flow"
    users_flow_dir.mkdir()
    users_flow_file = users_flow_dir / "flow.yml"
    users_flow_file.write_text(
        f"""
name: "users_flow"
home:
  type: "parquet"
  path: "{_path_for_yaml(sample_data_files['users'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(temp_config_dir / 'lake' / 'users')}"
"""
    )

    # Create orders_flow
    orders_flow_dir = flows_dir / "orders_flow"
    orders_flow_dir.mkdir()
    orders_flow_file = orders_flow_dir / "flow.yml"
    orders_flow_file.write_text(
        f"""
name: "orders_flow"
home:
  type: "parquet"
  path: "{_path_for_yaml(sample_data_files['orders'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(temp_config_dir / 'lake' / 'orders')}"
"""
    )

    return hygge_file


@pytest.fixture
def advanced_config_file(
    temp_config_dir: Path, sample_data_files: Dict[str, Path]
) -> Path:
    """Create advanced workspace configuration with custom options."""
    # Create hygge.yml
    hygge_file = temp_config_dir / "hygge.yml"
    hygge_file.write_text(
        """
name: "test_project"
flows_dir: "flows"
"""
    )

    # Create flows directory
    flows_dir = temp_config_dir / "flows"
    flows_dir.mkdir()

    # Create users_flow
    users_flow_dir = flows_dir / "users_flow"
    users_flow_dir.mkdir()
    users_flow_file = users_flow_dir / "flow.yml"
    users_flow_file.write_text(
        f"""
name: "users_flow"
home:
  type: "parquet"
  path: "{_path_for_yaml(sample_data_files['users'])}"
  options:
    batch_size: 500
store:
  type: "parquet"
  path: "{_path_for_yaml(temp_config_dir / 'lake' / 'users')}"
  options:
    batch_size: 1000
    compression: "snappy"
    file_pattern: "users_{{sequence:020d}}.parquet"
options:
  queue_size: 3
  timeout: 60
"""
    )

    # Create orders_flow
    orders_flow_dir = flows_dir / "orders_flow"
    orders_flow_dir.mkdir()
    orders_flow_file = orders_flow_dir / "flow.yml"
    orders_flow_file.write_text(
        f"""
name: "orders_flow"
home:
  type: "parquet"
  path: "{_path_for_yaml(sample_data_files['orders'])}"
  options:
    batch_size: 800
store:
  type: "parquet"
  path: "{_path_for_yaml(temp_config_dir / 'lake' / 'orders')}"
  options:
    batch_size: 1500
    compression: "gzip"
    file_pattern: "orders_{{sequence:020d}}.parquet"
options:
  queue_size: 5
  timeout: 120
"""
    )

    # Create products_flow
    products_flow_dir = flows_dir / "products_flow"
    products_flow_dir.mkdir()
    products_flow_file = products_flow_dir / "flow.yml"
    products_flow_file.write_text(
        f"""
name: "products_flow"
home:
  type: "parquet"
  path: "{_path_for_yaml(sample_data_files['products'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(temp_config_dir / 'lake' / 'products')}"
options:
  queue_size: 2
"""
    )

    return hygge_file


@pytest.fixture
def invalid_config_file(temp_config_dir: Path) -> Path:
    """Create invalid workspace configuration (references nonexistent paths)."""
    # Create hygge.yml
    hygge_file = temp_config_dir / "hygge.yml"
    hygge_file.write_text(
        """
name: "test_project"
flows_dir: "flows"
"""
    )

    # Create flows directory
    flows_dir = temp_config_dir / "flows"
    flows_dir.mkdir()

    # Create invalid_flow with nonexistent path
    invalid_flow_dir = flows_dir / "invalid_flow"
    invalid_flow_dir.mkdir()
    invalid_flow_file = invalid_flow_dir / "flow.yml"
    invalid_flow_file.write_text(
        """
name: "invalid_flow"
home:
  type: "parquet"
  path: "/nonexistent/path/file.parquet"
store:
  type: "parquet"
  path: "/nonexistent/path/destination"
"""
    )

    return hygge_file


class TestCoordinatorYAMLIntegration:
    """Test Coordinator with YAML configuration integration."""

    @pytest.mark.asyncio
    async def test_simple_config_execution(
        self, simple_config_file: Path, temp_config_dir: Path
    ):
        """Test Coordinator with simple workspace configuration."""
        # Given a simple workspace configuration
        coordinator = Coordinator(str(simple_config_file))

        # When running the coordinator
        await coordinator.run()

        # Then verify flows were created and executed
        assert len(coordinator.flows) == 2, "Should create 2 flows"
        assert coordinator.config is not None, "Configuration should be loaded"

        # Verify output directories exist
        users_output = temp_config_dir / "lake" / "users"
        orders_output = temp_config_dir / "lake" / "orders"

        assert users_output.exists(), "Users output directory should exist"
        assert orders_output.exists(), "Orders output directory should exist"

        # Verify output files were created
        users_files = list(users_output.glob("*.parquet"))
        orders_files = list(orders_output.glob("*.parquet"))

        assert len(users_files) > 0, "Users output files should be created"
        assert len(orders_files) > 0, "Orders output files should be created"

        # Verify data integrity
        total_users_rows = sum(len(pl.read_parquet(f)) for f in users_files)
        total_orders_rows = sum(len(pl.read_parquet(f)) for f in orders_files)

        assert total_users_rows == 1000, f"Users rows mismatch: {total_users_rows}"
        assert total_orders_rows == 2000, f"Orders rows mismatch: {total_orders_rows}"

    @pytest.mark.asyncio
    async def test_advanced_config_execution(
        self, advanced_config_file: Path, temp_config_dir: Path
    ):
        """Test Coordinator with advanced workspace configuration."""
        # Given an advanced workspace configuration
        coordinator = Coordinator(str(advanced_config_file))

        # When running the coordinator
        await coordinator.run()

        # Then verify all flows were created
        assert len(coordinator.flows) == 3, "Should create 3 flows"

        # Verify flow configurations
        flow_names = [flow.name for flow in coordinator.flows]
        assert "users_flow" in flow_names
        assert "orders_flow" in flow_names
        assert "products_flow" in flow_names

        # Verify output directories exist
        users_output = temp_config_dir / "lake" / "users"
        orders_output = temp_config_dir / "lake" / "orders"
        products_output = temp_config_dir / "lake" / "products"

        assert users_output.exists(), "Users output directory should exist"
        assert orders_output.exists(), "Orders output directory should exist"
        assert products_output.exists(), "Products output directory should exist"

        # Verify data integrity
        users_files = list(users_output.glob("*.parquet"))
        orders_files = list(orders_output.glob("*.parquet"))
        products_files = list(products_output.glob("*.parquet"))

        total_users_rows = sum(len(pl.read_parquet(f)) for f in users_files)
        total_orders_rows = sum(len(pl.read_parquet(f)) for f in orders_files)
        total_products_rows = sum(len(pl.read_parquet(f)) for f in products_files)

        assert total_users_rows == 1000, f"Users rows mismatch: {total_users_rows}"
        assert total_orders_rows == 2000, f"Orders rows mismatch: {total_orders_rows}"
        assert (
            total_products_rows == 500
        ), f"Products rows mismatch: {total_products_rows}"

    @pytest.mark.asyncio
    async def test_config_validation(self, temp_config_dir: Path):
        """Test configuration validation."""
        # Test empty flows directory
        hygge_file = temp_config_dir / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        # Create empty flows directory
        flows_dir = temp_config_dir / "flows"
        flows_dir.mkdir()

        coordinator = Coordinator(str(hygge_file))

        with pytest.raises(ConfigError, match="No flows found in directory"):
            await coordinator.run()

    @pytest.mark.asyncio
    async def test_invalid_config_handling(self, invalid_config_file: Path):
        """Test Coordinator handles invalid configurations gracefully."""
        coordinator = Coordinator(str(invalid_config_file))

        # Should raise an error for invalid configuration (HomeReadError when path is opened)
        with pytest.raises((ConfigError, FileNotFoundError, HomeReadError)):
            await coordinator.run()

    @pytest.mark.asyncio
    async def test_coordinator_parallel_execution(
        self, advanced_config_file: Path, temp_config_dir: Path
    ):
        """Test that Coordinator runs flows in parallel."""
        # Given an advanced workspace configuration
        coordinator = Coordinator(str(advanced_config_file))

        # Track execution time
        start_time = asyncio.get_event_loop().time()
        await coordinator.run()
        end_time = asyncio.get_event_loop().time()

        duration = end_time - start_time

        # Should complete reasonably quickly (parallel execution)
        assert duration < 30, f"Execution took too long: {duration:.2f}s"

        # Verify all flows completed
        assert len(coordinator.flows) == 3, "All flows should be created"

        # Verify all outputs exist
        outputs = [
            temp_config_dir / "lake" / "users",
            temp_config_dir / "lake" / "orders",
            temp_config_dir / "lake" / "products",
        ]

        for output_dir in outputs:
            assert output_dir.exists(), f"Output directory should exist: {output_dir}"
            files = list(output_dir.glob("*.parquet"))
            assert len(files) > 0, f"Output files should exist: {output_dir}"

    @pytest.mark.asyncio
    async def test_coordinator_error_isolation(
        self, temp_config_dir: Path, sample_data_files: Dict[str, Path]
    ):
        """Test that Coordinator handles individual flow errors appropriately."""
        # Create workspace with one valid and one invalid flow
        hygge_file = temp_config_dir / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        flows_dir = temp_config_dir / "flows"
        flows_dir.mkdir()

        # Create valid_flow
        valid_flow_dir = flows_dir / "valid_flow"
        valid_flow_dir.mkdir()
        valid_flow_file = valid_flow_dir / "flow.yml"
        valid_flow_file.write_text(
            f"""
name: "valid_flow"
home:
  type: "parquet"
  path: "{_path_for_yaml(sample_data_files['users'])}"
store:
  type: "parquet"
  path: "{_path_for_yaml(temp_config_dir / 'valid_output')}"
"""
        )

        # Create invalid_flow
        invalid_flow_dir = flows_dir / "invalid_flow"
        invalid_flow_dir.mkdir()
        invalid_flow_file = invalid_flow_dir / "flow.yml"
        invalid_flow_file.write_text(
            """
name: "invalid_flow"
home:
  type: "parquet"
  path: "/nonexistent/file.parquet"
store:
  type: "parquet"
  path: "/nonexistent/path/destination"
"""
        )

        coordinator = Coordinator(str(hygge_file))

        # Should raise an error when any flow fails
        with pytest.raises(Exception):
            await coordinator.run()

    @pytest.mark.asyncio
    async def test_coordinator_flow_options(self, simple_config_file: Path):
        """Test that Coordinator respects flow options from configuration."""
        coordinator = Coordinator(str(simple_config_file))

        # Prepare configuration and create flows
        coordinator.config = coordinator._workspace.prepare()
        coordinator._create_flows()

        # Verify flow options
        assert len(coordinator.flows) == 2

        for flow in coordinator.flows:
            # Should have default queue size
            assert flow.queue_size == 10  # Default
            assert flow.timeout == 300  # Default
            assert flow.name in ["users_flow", "orders_flow"]
