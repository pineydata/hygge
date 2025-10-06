"""
Simple unit tests for Coordinator class.

These tests focus on testing actual behavior rather than complex mocking.
They verify that the registry pattern works correctly with real configurations.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from hygge.core.coordinator import Coordinator, CoordinatorConfig, validate_config
from hygge.core.flow import FlowConfig

# Import Parquet implementations to register them


class TestCoordinatorConfig:
    """Test CoordinatorConfig validation and creation."""

    def test_coordinator_config_creation(self):
        """Test that CoordinatorConfig can be created with valid flows."""
        config_data = {
            "flows": {
                "test_flow": {"home": "data/test.parquet", "store": "output/test"}
            }
        }

        config = CoordinatorConfig.from_dict(config_data)
        assert len(config.flows) == 1
        assert "test_flow" in config.flows
        assert isinstance(config.flows["test_flow"], FlowConfig)

    def test_coordinator_config_empty_flows_validation(self):
        """Test that empty flows are rejected."""
        config_data = {"flows": {}}

        with pytest.raises(ValueError, match="At least one flow must be configured"):
            CoordinatorConfig.from_dict(config_data)

    def test_coordinator_config_get_flow_config(self):
        """Test getting specific flow configuration."""
        config_data = {
            "flows": {
                "flow1": {"home": "data/flow1.parquet", "store": "output/flow1"},
                "flow2": {"home": "data/flow2.parquet", "store": "output/flow2"},
            }
        }

        config = CoordinatorConfig.from_dict(config_data)

        # Test existing flow
        flow1_config = config.get_flow_config("flow1")
        assert isinstance(flow1_config, FlowConfig)

        # Test non-existing flow
        with pytest.raises(ValueError, match="Flow 'nonexistent' not found"):
            config.get_flow_config("nonexistent")


class TestValidateConfig:
    """Test configuration validation function."""

    def test_validate_config_success(self):
        """Test successful configuration validation."""
        config = {
            "flows": {
                "test_flow": {"home": "data/test.parquet", "store": "output/test"}
            }
        }

        errors = validate_config(config)
        assert errors == []

    def test_validate_config_failure(self):
        """Test configuration validation failure."""
        config = {
            "flows": {}  # Empty flows should fail
        }

        errors = validate_config(config)
        assert len(errors) > 0
        assert "At least one flow must be configured" in errors[0]


class TestCoordinatorInitialization:
    """Test Coordinator initialization and basic setup."""

    def test_coordinator_initialization_with_file(self):
        """Test Coordinator initialization with file path."""
        # Create a temporary config file
        config_data = {
            "flows": {
                "test_flow": {"home": "data/test.parquet", "store": "output/test"}
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name

        try:
            coordinator = Coordinator(config_file)
            assert coordinator.config_path == Path(config_file)
            assert coordinator.config is None  # Not loaded yet
            assert coordinator.flows == []
            assert coordinator.options == {}
        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_coordinator_initialization_with_directory(self):
        """Test Coordinator initialization with directory path."""
        # Create a temporary directory with flow config
        with tempfile.TemporaryDirectory() as temp_dir:
            flow_dir = Path(temp_dir) / "test_flow"
            flow_dir.mkdir()

            flow_config = {"home": "data/test.parquet", "store": "output/test"}

            with open(flow_dir / "flow.yml", "w") as f:
                yaml.dump(flow_config, f)

            coordinator = Coordinator(temp_dir)
            assert coordinator.config_path == Path(temp_dir)
            assert coordinator.config is None  # Not loaded yet
            assert coordinator.flows == []


class TestCoordinatorConfigLoading:
    """Test Coordinator configuration loading."""

    def test_load_single_file_config(self):
        """Test loading configuration from single file."""
        config_data = {
            "flows": {
                "test_flow": {"home": "data/test.parquet", "store": "output/test"}
            },
            "options": {"continue_on_error": True},
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name

        try:
            coordinator = Coordinator(config_file)
            coordinator._load_single_file_config()

            assert coordinator.config is not None
            assert len(coordinator.config.flows) == 1
            assert "test_flow" in coordinator.config.flows
            assert coordinator.options == {"continue_on_error": True}
        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_load_directory_config(self):
        """Test loading configuration from directory structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create flow directory
            flow_dir = Path(temp_dir) / "test_flow"
            flow_dir.mkdir()

            flow_config = {"home": "data/test.parquet", "store": "output/test"}

            with open(flow_dir / "flow.yml", "w") as f:
                yaml.dump(flow_config, f)

            # Create options file
            options = {"continue_on_error": True}
            with open(Path(temp_dir) / "options.yml", "w") as f:
                yaml.dump(options, f)

            coordinator = Coordinator(temp_dir)
            coordinator._load_directory_config()

            assert coordinator.config is not None
            assert len(coordinator.config.flows) == 1
            assert "test_flow" in coordinator.config.flows
            assert coordinator.options == {"continue_on_error": True}

    def test_load_config_file_not_found(self):
        """Test loading configuration when file doesn't exist."""
        coordinator = Coordinator("/nonexistent/file.yaml")

        with pytest.raises(
            Exception, match="Configuration path must be file or directory"
        ):
            coordinator._load_config()

    def test_load_config_invalid_yaml(self):
        """Test loading configuration with invalid YAML."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            config_file = f.name

        try:
            coordinator = Coordinator(config_file)

            with pytest.raises(Exception, match="Failed to load configuration"):
                coordinator._load_config()
        finally:
            Path(config_file).unlink(missing_ok=True)


class TestCoordinatorFlowCreation:
    """Test Coordinator flow creation."""

    def test_create_flows_from_config(self):
        """Test creating flows from configuration."""
        config_data = {
            "flows": {
                "flow1": {"home": "data/flow1.parquet", "store": "output/flow1"},
                "flow2": {"home": "data/flow2.parquet", "store": "output/flow2"},
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name

        try:
            coordinator = Coordinator(config_file)
            coordinator._load_config()
            coordinator._create_flows()

            assert len(coordinator.flows) == 2
            assert coordinator.flows[0].name == "flow1"
            assert coordinator.flows[1].name == "flow2"

            # Verify flows have the right components
            for flow in coordinator.flows:
                assert flow.home is not None
                assert flow.store is not None
                assert flow.home.config.type == "parquet"
                assert flow.store.config.type == "parquet"
        finally:
            Path(config_file).unlink(missing_ok=True)


class TestCoordinatorIntegration:
    """Test Coordinator integration with registry pattern."""

    def test_coordinator_with_simple_config(self):
        """Test Coordinator with simple string-based configuration."""
        config_data = {
            "flows": {
                "simple_flow": {"home": "data/simple.parquet", "store": "output/simple"}
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name

        try:
            coordinator = Coordinator(config_file)
            coordinator._load_config()
            coordinator._create_flows()

            # Verify the flow was created correctly
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]

            # Verify registry pattern worked
            assert flow.home.config.type == "parquet"
            assert flow.store.config.type == "parquet"
            assert flow.home.config.path == "data/simple.parquet"
            assert flow.store.config.path == "output/simple"
        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_coordinator_with_dict_config(self):
        """Test Coordinator with dictionary-based configuration."""
        config_data = {
            "flows": {
                "dict_flow": {
                    "home": {
                        "type": "parquet",
                        "path": "data/dict.parquet",
                        "batch_size": 5000,
                    },
                    "store": {
                        "type": "parquet",
                        "path": "output/dict",
                        "compression": "snappy",
                    },
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name

        try:
            coordinator = Coordinator(config_file)
            coordinator._load_config()
            coordinator._create_flows()

            # Verify the flow was created correctly
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]

            # Verify registry pattern worked with custom options
            assert flow.home.config.type == "parquet"
            assert flow.store.config.type == "parquet"
            assert flow.home.config.batch_size == 5000
            assert flow.store.config.compression == "snappy"
        finally:
            Path(config_file).unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
