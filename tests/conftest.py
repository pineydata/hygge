"""
Common test fixtures and configuration.

This module provides shared fixtures and utilities following hygge's testing philosophy:
- Focus on user experience and data integrity
- Provide comfortable defaults for tests
- Keep fixtures simple and maintainable
"""
import pytest
import logging
import sys
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any
import polars as pl
from datetime import datetime

# Add src directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture(autouse=True)
def setup_logging():
    """Configure logging for tests."""
    logging.basicConfig(level=logging.DEBUG)
    yield


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_data_dir():
    """Path to test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture
def small_sample_data():
    """Generate small sample data for testing."""
    return pl.DataFrame({
        "id": range(10),
        "value": [f"test_{i}" for i in range(10)],
        "timestamp": [datetime.now() for _ in range(10)]
    })


@pytest.fixture
def medium_sample_data():
    """Generate medium sample data for testing."""
    return pl.DataFrame({
        "id": range(1000),
        "value": [f"test_{i}" for i in range(1000)],
        "timestamp": [datetime.now() for _ in range(1000)],
        "category": [f"cat_{i % 5}" for i in range(1000)]
    })


@pytest.fixture
def sample_parquet_file(temp_dir, small_sample_data):
    """Create a sample parquet file for testing."""
    file_path = temp_dir / "test_data.parquet"
    small_sample_data.write_parquet(file_path)
    return file_path


@pytest.fixture
def simple_flow_config():
    """Simple test configuration - Rails spirit."""
    return {
        "flows": {
            "test_flow": {
                "home": "data/source/test.parquet",
                "store": "data/destination/output"
            }
        }
    }


@pytest.fixture
def advanced_flow_config():
    """Advanced test configuration with options."""
    return {
        "flows": {
            "advanced_flow": {
                "home": {
                    "type": "parquet",
                    "path": "data/source/users.parquet",
                    "options": {
                        "batch_size": 5000
                    }
                },
                "store": {
                    "type": "parquet",
                    "path": "data/lake/users",
                    "options": {
                        "compression": "snappy",
                        "batch_size": 10000
                    }
                },
                "options": {
                    "timeout": 300
                }
            }
        }
    }


@pytest.fixture
def multiple_flows_config():
    """Configuration with multiple flows."""
    return {
        "flows": {
            "users_flow": {
                "home": "data/source/users.parquet",
                "store": "data/lake/users"
            },
            "orders_flow": {
                "home": "data/source/orders.parquet",
                "store": "data/lake/orders"
            },
            "products_flow": {
                "home": {
                    "type": "parquet",
                    "path": "data/source/products.parquet",
                    "options": {"batch_size": 1000}
                },
                "store": "data/lake/products"
            }
        }
    }


@pytest.fixture
def sql_config():
    """Configuration with SQL home."""
    return {
        "flows": {
            "sql_flow": {
                "home": {
                    "type": "sql",
                    "table": "users",
                    "connection": "sqlite:///test.db",
                    "options": {"batch_size": 1000}
                },
                "store": "data/lake/users"
            }
        }
    }


@pytest.fixture
def invalid_config():
    """Configuration with validation errors."""
    return {
        "flows": {
            "bad_flow": {
                "home": {
                    "type": "invalid_type",
                    "path": "data/test.parquet"
                },
                "store": {
                    "type": "invalid_type",
                    "path": "data/output"
                }
            }
        }
    }


@pytest.fixture
def hygge_settings_defaults():
    """Default HyggeSettings for testing."""
    from hygge.core.configs.settings import HyggeSettings
    return HyggeSettings()


@pytest.fixture
def hygge_settings_custom():
    """Custom HyggeSettings for testing."""
    from hygge.core.configs.settings import HyggeSettings
    return HyggeSettings(
        home_type="sql",
        store_type="parquet",
        home_batch_size=5000,
        store_batch_size=50000,
        flow_queue_size=5
    )


class TestDataManager:
    """Helper class for managing test data."""

    def __init__(self, temp_dir: Path):
        self.temp_dir = temp_dir
        self.home_dir = temp_dir / "home"
        self.store_dir = temp_dir / "store"
        self.setup_directories()

    def setup_directories(self):
        """Create necessary test directories."""
        self.home_dir.mkdir(parents=True, exist_ok=True)
        self.store_dir.mkdir(parents=True, exist_ok=True)

    def create_parquet_file(self, filename: str, data: pl.DataFrame) -> Path:
        """Create a parquet file for testing."""
        file_path = self.home_dir / filename
        data.write_parquet(file_path)
        return file_path

    def write_test_data(self, batch_size: int = 100) -> Path:
        """Write test data and return file path."""
        data = pl.DataFrame({
            "id": range(batch_size),
            "value": [f"test_value_{i}" for i in range(batch_size)],
            "timestamp": [datetime.now() for _ in range(batch_size)]
        })
        return self.create_parquet_file("test_data.parquet", data)

    def cleanup(self):
        """Clean up test directories."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)


@pytest.fixture
def test_data_manager(temp_dir):
    """Test data manager fixture."""
    manager = TestDataManager(temp_dir)
    yield manager
    manager.cleanup()


class ConfigValidator:
    """Helper class for validating configurations."""

    @staticmethod
    def validate_simple_config(config: Dict[str, Any]) -> bool:
        """Validate simple configuration format."""
        if "flows" not in config:
            return False

        for flow_name, flow_config in config["flows"].items():
            if "home" not in flow_config or "store" not in flow_config:
                return False

            # Check if configs are simple strings
            home = flow_config["home"]
            store = flow_config["store"]

            if not isinstance(home, str) or not isinstance(store, str):
                return False

        return True

    @staticmethod
    def validate_advanced_config(config: Dict[str, Any]) -> bool:
        """Validate advanced configuration format."""
        if "flows" not in config:
            return False

        for flow_name, flow_config in config["flows"].items():
            if "home" not in flow_config or "store" not in flow_config:
                return False

            # Check if configs are dictionaries
            home = flow_config["home"]
            store = flow_config["store"]

            if not isinstance(home, dict) or not isinstance(store, dict):
                return False

            # Check required fields
            if "type" not in home or "path" not in home:
                return False
            if "type" not in store or "path" not in store:
                return False

        return True


@pytest.fixture
def config_validator():
    """Config validator fixture."""
    return ConfigValidator()


class TestAssertionHelpers:
    """Helper class for common test assertions."""

    @staticmethod
    def assert_dataframe_equal(df1: pl.DataFrame, df2: pl.DataFrame):
        """Assert two DataFrames are equal."""
        assert df1.equals(df2), f"DataFrames not equal. Shape: {df1.shape} vs {df2.shape}"

    @staticmethod
    def assert_parquet_file_exists(path: Path):
        """Assert parquet file exists and is readable."""
        assert path.exists(), f"File {path} does not exist"
        assert path.suffix == ".parquet", f"File {path} is not a parquet file"

        # Try to read it
        try:
            pl.read_parquet(path)
        except Exception as e:
            pytest.fail(f"Could not read parquet file {path}: {e}")

    @staticmethod
    def assert_config_valid(config: Dict[str, Any]):
        """Assert configuration is valid."""
        assert isinstance(config, dict), "Config must be a dictionary"
        assert "flows" in config, "Config must contain 'flows'"
        assert len(config["flows"]) > 0, "Config must have at least one flow"

        for flow_name in config["flows"]:
            assert isinstance(flow_name, str), "Flow name must be a string"
            assert len(flow_name.strip()) > 0, "Flow name cannot be empty"


@pytest.fixture
def assertion_helpers():
    """Test assertion helpers fixture."""
    return TestAssertionHelpers()