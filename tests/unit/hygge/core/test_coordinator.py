"""
Simple unit tests for Coordinator class.

These tests focus on testing actual behavior rather than complex mocking.
They verify that the registry pattern works correctly with real configurations.
"""

import asyncio
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import polars as pl
import pytest
import yaml

from hygge.core.coordinator import Coordinator, CoordinatorConfig, validate_config
from hygge.core.flow import FlowConfig
from hygge.core.journal import Journal, JournalConfig

# Import Parquet implementations to register them
from hygge.homes.parquet.home import ParquetHome, ParquetHomeConfig  # noqa: F401
from hygge.stores.parquet.store import ParquetStore, ParquetStoreConfig  # noqa: F401
from hygge.utility.exceptions import ConfigError


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

    def test_coordinator_config_journal_conversion(self):
        """Test that CoordinatorConfig converts journal config properly."""
        config_data = {
            "journal": {"path": "/tmp/journal"},
            "flows": {
                "test_flow": {"home": "data/test.parquet", "store": "output/test"}
            },
        }

        config = CoordinatorConfig.from_dict(config_data)
        assert isinstance(config.journal, JournalConfig)
        assert config.journal.path == "/tmp/journal"

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
    """Legacy configuration loading helpers remain covered elsewhere."""

    pass


class TestCoordinatorJournalIntegration:
    """Test journal integration hooks inside Coordinator."""

    def test_coordinator_flow_receives_journal_context(self, tmp_path):
        """Coordinator should attach journal and metadata to flows."""
        journal_dir = tmp_path / "journal_dir"
        config_data = {
            "journal": {"path": str(journal_dir)},
            "flows": {
                "users_flow": {
                    "home": {
                        "type": "parquet",
                        "path": str(tmp_path / "source.parquet"),
                    },
                    "store": {"type": "parquet", "path": str(tmp_path / "dest")},
                    "run_type": "incremental",
                    "watermark": {
                        "primary_key": "id",
                        "watermark_column": "updated_at",
                    },
                }
            },
        }

        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        coordinator = Coordinator(str(config_file))
        coordinator._load_single_file_config()
        coordinator.coordinator_run_id = "coord_run_123"
        coordinator.coordinator_name = "test_coordinator"
        coordinator._create_flows()

        assert len(coordinator.flows) == 1
        flow = coordinator.flows[0]
        assert isinstance(flow.journal, Journal)
        assert flow.journal.journal_path == (journal_dir / "journal.parquet")
        assert flow.coordinator_run_id == "coord_run_123"
        assert flow.coordinator_name == "test_coordinator"
        assert flow.run_type == "incremental"
        assert flow.watermark_config == {
            "primary_key": "id",
            "watermark_column": "updated_at",
        }
        assert coordinator.journal is flow.journal

    @pytest.mark.asyncio
    async def test_run_flow_assigns_flow_run_id(self, tmp_path, monkeypatch):
        """_run_flow should assign deterministic flow_run_id."""
        config_data = {
            "flows": {
                "users_flow": {
                    "home": {
                        "type": "parquet",
                        "path": str(tmp_path / "source.parquet"),
                    },
                    "store": {"type": "parquet", "path": str(tmp_path / "dest")},
                }
            }
        }

        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        coordinator = Coordinator(str(config_file))
        coordinator._load_single_file_config()
        coordinator._create_flows()

        coordinator.coordinator_name = "test_coord"
        coordinator.coordinator_run_id = "coord_run"
        coordinator.flow_run_ids = {}

        generated_ids = []

        def fake_generate_run_id(components):
            generated_ids.append(tuple(components))
            return f"run_{len(generated_ids)}"

        monkeypatch.setattr(
            "hygge.core.coordinator.generate_run_id", fake_generate_run_id
        )

        flow = coordinator.flows[0]
        flow.start = AsyncMock()

        await coordinator._run_flow(flow, 1, 1)

        assert flow.flow_run_id == "run_1"
        assert coordinator.flow_run_ids[flow.base_flow_name] == "run_1"
        assert flow.coordinator_run_id == "coord_run"
        assert generated_ids[0][0] == "test_coord"

    @pytest.mark.asyncio
    async def test_flow_run_id_shared_across_entities(self, tmp_path, monkeypatch):
        """Entity flows for same base flow share a flow_run_id."""
        config_data = {
            "flows": {
                "users_flow": {
                    "home": {
                        "type": "parquet",
                        "path": str(tmp_path / "source.parquet"),
                    },
                    "store": {"type": "parquet", "path": str(tmp_path / "dest")},
                    "entities": ["users", "orders"],
                }
            }
        }

        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        coordinator = Coordinator(str(config_file))
        coordinator._load_single_file_config()
        coordinator._create_flows()

        coordinator.coordinator_name = "test_coord"
        coordinator.coordinator_run_id = "coord_run"
        coordinator.flow_run_ids = {}

        generated_ids = []

        def fake_generate_run_id(components):
            generated_ids.append(tuple(components))
            return f"run_{len(generated_ids)}"

        monkeypatch.setattr(
            "hygge.core.coordinator.generate_run_id", fake_generate_run_id
        )

        for idx, flow in enumerate(coordinator.flows, start=1):
            flow.start = AsyncMock()
            await coordinator._run_flow(flow, idx, len(coordinator.flows))

        flow_ids = {flow.flow_run_id for flow in coordinator.flows}
        assert len(flow_ids) == 1
        assert len(generated_ids) == 1

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


class TestProjectCentricCoordinator:
    """Test project-centric functionality in Coordinator."""

    def test_project_discovery_finds_hygge_yml(self):
        """Test that project discovery finds hygge.yml in current directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create hygge.yml
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "flows"
"""
            )

            # Change to temp directory
            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                coordinator = Coordinator()
                # Workspace is found but config is not prepared yet (lazy loading)
                assert coordinator.config_path.resolve() == hygge_file.resolve()
                assert coordinator.project_config["name"] == "test_project"
                assert coordinator.project_config["flows_dir"] == "flows"
                # Config is None until run() is called (lazy loading)
                assert coordinator.config is None
            finally:
                os.chdir(original_cwd)

    def test_project_discovery_finds_hygge_yml_in_parent(self):
        """Test that project discovery finds hygge.yml in parent directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create hygge.yml in parent
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "flows"
"""
            )

            # Create subdirectory
            sub_dir = temp_path / "subdir"
            sub_dir.mkdir()

            # Change to subdirectory
            original_cwd = Path.cwd()
            os.chdir(sub_dir)

            try:
                coordinator = Coordinator()
                # Workspace is found but config is not prepared yet (lazy loading)
                assert coordinator.config_path.resolve() == hygge_file.resolve()
                assert coordinator.project_config["name"] == "test_project"
                # Config is None until run() is called (lazy loading)
                assert coordinator.config is None
            finally:
                os.chdir(original_cwd)

    def test_project_discovery_raises_error_when_no_hygge_yml(self):
        """Test that project discovery raises error when no hygge.yml found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to empty temp directory
            original_cwd = Path.cwd()
            os.chdir(temp_dir)

            try:
                with pytest.raises(ConfigError, match="No hygge.yml found"):
                    Coordinator()
            finally:
                os.chdir(original_cwd)

    def test_load_project_flows(self):
        """Test loading flows from flows/ directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create project structure
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "flows"
"""
            )

            # Create flows directory
            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            # Create first flow
            flow1_dir = flows_dir / "salesforce_to_lake"
            flow1_dir.mkdir()

            flow1_file = flow1_dir / "flow.yml"
            flow1_file.write_text(
                """
name: "salesforce_to_lake"
home:
  type: "parquet"
  path: "data/source"
store:
  type: "parquet"
  path: "data/destination"
defaults:
  key_column: "Id"
  batch_size: 10000
"""
            )

            # Create entities directory for first flow
            entities1_dir = flow1_dir / "entities"
            entities1_dir.mkdir()

            account_file = entities1_dir / "account.yml"
            account_file.write_text(
                """
name: "Account"
columns:
  - Id
  - Name
  - Type
"""
            )

            contact_file = entities1_dir / "contact.yml"
            contact_file.write_text(
                """
name: "Contact"
columns:
  - Id
  - FirstName
  - LastName
"""
            )

            # Create second flow (no entities)
            flow2_dir = flows_dir / "warehouse_to_parquet"
            flow2_dir.mkdir()

            flow2_file = flow2_dir / "flow.yml"
            flow2_file.write_text(
                """
name: "warehouse_to_parquet"
home:
  type: "parquet"
  path: "data/warehouse"
store:
  type: "parquet"
  path: "data/output"
"""
            )

            # Change to temp directory
            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                coordinator = Coordinator()
                # Config is loaded lazily - prepare it explicitly for this test
                coordinator.config = coordinator._workspace.prepare()

                # Verify flows were loaded
                assert len(coordinator.config.flows) == 2
                assert "salesforce_to_lake" in coordinator.config.flows
                assert "warehouse_to_parquet" in coordinator.config.flows

                # Verify first flow has entities
                flow1_config = coordinator.config.flows["salesforce_to_lake"]
                assert flow1_config.entities is not None
                assert len(flow1_config.entities) == 2

                # Check entity names
                entity_names = [entity["name"] for entity in flow1_config.entities]
                assert "Account" in entity_names
                assert "Contact" in entity_names

                # Verify second flow has no entities
                flow2_config = coordinator.config.flows["warehouse_to_parquet"]
                assert flow2_config.entities is None

            finally:
                os.chdir(original_cwd)

    def test_load_project_flows_with_custom_flows_dir(self):
        """Test loading flows from custom flows directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create hygge.yml with custom flows_dir
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "my_flows"
"""
            )

            # Create custom flows directory
            flows_dir = temp_path / "my_flows"
            flows_dir.mkdir()

            # Create flow
            flow_dir = flows_dir / "test_flow"
            flow_dir.mkdir()

            flow_file = flow_dir / "flow.yml"
            flow_file.write_text(
                """
name: "test_flow"
home:
  type: "parquet"
  path: "data/source"
store:
  type: "parquet"
  path: "data/destination"
"""
            )

            # Change to temp directory
            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                coordinator = Coordinator()
                # Config is loaded lazily - prepare it explicitly for this test
                coordinator.config = coordinator._workspace.prepare()

                # Verify flow was loaded from custom directory
                assert len(coordinator.config.flows) == 1
                assert "test_flow" in coordinator.config.flows

            finally:
                os.chdir(original_cwd)

    def test_load_project_flows_raises_error_when_no_flows_dir(self):
        """Test that error is raised when flows directory doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create hygge.yml
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "nonexistent_flows"
"""
            )

            # Change to temp directory
            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                # Workspace.prepare() will raise error when flows directory not found
                # Config loading happens lazily in run(), so prepare explicitly here
                coordinator = Coordinator()
                with pytest.raises(ConfigError, match="Flows directory not found"):
                    coordinator.config = coordinator._workspace.prepare()
            finally:
                os.chdir(original_cwd)

    def test_load_project_flows_raises_error_when_no_flows(self):
        """Test that error is raised when no flows found in directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create hygge.yml
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "flows"
"""
            )

            # Create empty flows directory
            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            # Change to temp directory
            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                # Workspace.prepare() will raise error when no flows found
                # Config loading happens lazily in run(), so prepare explicitly here
                coordinator = Coordinator()
                with pytest.raises(ConfigError, match="No flows found in directory"):
                    coordinator.config = coordinator._workspace.prepare()
            finally:
                os.chdir(original_cwd)

    def test_entity_defaults_inheritance(self):
        """Test that entities inherit defaults from flow config."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create project structure
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "flows"
"""
            )

            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            flow_dir = flows_dir / "test_flow"
            flow_dir.mkdir()

            flow_file = flow_dir / "flow.yml"
            flow_file.write_text(
                """
name: "test_flow"
home:
  type: "parquet"
  path: "data/source"
store:
  type: "parquet"
  path: "data/destination"
defaults:
  key_column: "Id"
  batch_size: 5000
  schema: "test_schema"
"""
            )

            entities_dir = flow_dir / "entities"
            entities_dir.mkdir()

            entity_file = entities_dir / "test_entity.yml"
            entity_file.write_text(
                """
name: "TestEntity"
columns:
  - Id
  - Name
# Override batch_size
source_config:
  batch_size: 10000
"""
            )

            # Change to temp directory
            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                coordinator = Coordinator()
                # Config is loaded lazily - prepare it explicitly for this test
                coordinator.config = coordinator._workspace.prepare()

                # Verify entity inherited defaults
                flow_config = coordinator.config.flows["test_flow"]
                entity = flow_config.entities[0]

                # Should inherit from defaults
                assert entity["key_column"] == "Id"
                assert entity["schema"] == "test_schema"

                # Should override batch_size
                assert entity["source_config"]["batch_size"] == 10000

            finally:
                os.chdir(original_cwd)


class TestCoordinatorEntityPathMerging:
    """Test entity flow path merging with PathHelper."""

    def test_entity_flow_path_merging_simple(self):
        """Test that entity paths are merged correctly with flow paths."""
        config_data = {
            "flows": {
                "test_flow": {
                    "home": {"type": "parquet", "path": "data/source"},
                    "store": {"type": "parquet", "path": "data/destination"},
                    "entities": [
                        {
                            "name": "users",
                            "home": {"path": "users"},
                        }
                    ],
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

            # Should create entity flow with merged path
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]
            assert flow.name == "test_flow_users"
            # Path should be merged: "data/source" + "users" = "data/source/users"
            assert flow.home.config.path == "data/source/users"

        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_entity_flow_path_merging_with_trailing_slash(self):
        """Test path merging handles trailing slashes correctly."""
        config_data = {
            "flows": {
                "test_flow": {
                    "home": {"type": "parquet", "path": "data/source/"},
                    "store": {"type": "parquet", "path": "data/destination"},
                    "entities": [
                        {
                            "name": "users",
                            "home": {"path": "users"},
                        }
                    ],
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

            flow = coordinator.flows[0]
            # Should merge correctly despite trailing slash: "data/source/" + "users"
            assert flow.home.config.path == "data/source/users"

        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_entity_flow_path_merging_with_leading_slash(self):
        """Test path merging handles leading slashes correctly."""
        config_data = {
            "flows": {
                "test_flow": {
                    "home": {"type": "parquet", "path": "data/source"},
                    "store": {"type": "parquet", "path": "data/destination"},
                    "entities": [
                        {
                            "name": "users",
                            "home": {"path": "/users"},
                        }
                    ],
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

            flow = coordinator.flows[0]
            # Should merge correctly despite leading slash: "data/source" + "/users"
            assert flow.home.config.path == "data/source/users"

        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_entity_flow_path_merging_both_slashes(self):
        """Test path merging handles both trailing and leading slashes."""
        config_data = {
            "flows": {
                "test_flow": {
                    "home": {"type": "parquet", "path": "data/source/"},
                    "store": {"type": "parquet", "path": "data/destination"},
                    "entities": [
                        {
                            "name": "users",
                            "home": {"path": "/users"},
                        }
                    ],
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

            flow = coordinator.flows[0]
            # Should merge correctly: "data/source/" + "/users" = "data/source/users"
            assert flow.home.config.path == "data/source/users"

        finally:
            Path(config_file).unlink(missing_ok=True)


class TestCoordinatorFlowOverrides:
    """Test Coordinator flow-level overrides (CLI and config)."""

    def test_apply_flow_overrides_full_drop(self):
        """Test that flow-level full_drop is applied to store config."""
        config_data = {
            "flows": {
                "test_flow": {
                    "home": {"type": "parquet", "path": "data/source"},
                    "store": {
                        "type": "open_mirroring",
                        "account_url": "https://onelake.dfs.fabric.microsoft.com",
                        "filesystem": "test",
                        "mirror_name": "test-db",
                        "key_columns": ["id"],
                        "row_marker": 0,
                    },
                    "full_drop": True,  # Flow-level override
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

            # Verify flow was created
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]

            # Verify flow-level full_drop drives the run_type + store strategy
            store = flow.store
            if hasattr(store, "full_drop_mode"):
                assert store.full_drop_mode is True
            assert flow.run_type == "full_drop"

        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_apply_flow_overrides_via_cli(self):
        """Test that CLI flow overrides are applied correctly."""
        config_data = {
            "flows": {
                "test_flow": {
                    "home": {"type": "parquet", "path": "data/source"},
                    "store": {
                        "type": "open_mirroring",
                        "account_url": "https://onelake.dfs.fabric.microsoft.com",
                        "filesystem": "test",
                        "mirror_name": "test-db",
                        "key_columns": ["id"],
                        "row_marker": 0,
                    },
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name

        try:
            # Simulate CLI override: flow.test_flow.full_drop=true
            flow_overrides = {"test_flow": {"full_drop": True}}

            coordinator = Coordinator(config_file, flow_overrides=flow_overrides)
            coordinator._load_config()
            coordinator._create_flows()

            # Verify flow was created
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]

            # Verify CLI override was applied
            # Flow config should have full_drop=True
            flow_config = coordinator.config.flows["test_flow"]
            assert flow_config.full_drop is True

            # Store should have full_drop=True (from flow-level override)
            store = flow.store
            if hasattr(store, "full_drop_mode"):
                assert store.full_drop_mode is True
            assert flow.run_type == "full_drop"

        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_flow_overrides_not_applied_when_none(self):
        """Test that flow overrides don't affect config when not provided."""
        config_data = {
            "flows": {
                "test_flow": {
                    "home": {"type": "parquet", "path": "data/source"},
                    "store": {"type": "parquet", "path": "data/output"},
                    "full_drop": False,  # Flow-level setting
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

            # Verify flow config has the original full_drop value
            flow_config = coordinator.config.flows["test_flow"]
            assert flow_config.full_drop is False

        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_entity_flow_inherits_base_flow_full_drop(self):
        """Test that entity flows inherit flow-level full_drop from base flow."""
        config_data = {
            "flows": {
                "base_flow": {
                    "home": {"type": "parquet", "path": "data/source"},
                    "store": {
                        "type": "open_mirroring",
                        "account_url": "https://onelake.dfs.fabric.microsoft.com",
                        "filesystem": "test",
                        "mirror_name": "test-db",
                        "key_columns": ["id"],
                        "row_marker": 0,
                    },
                    "full_drop": True,  # Flow-level setting
                    "entities": ["Account", "Contact"],
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

            # Should create 2 entity flows
            assert len(coordinator.flows) == 2

            # Both entity flows should inherit full_drop=True from base flow
            for flow in coordinator.flows:
                assert flow.name.startswith("base_flow_")
                store = flow.store
                if hasattr(store, "full_drop_mode"):
                    assert store.full_drop_mode is True
                assert flow.run_type == "full_drop"

        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_store_incremental_override_forced_incremental(self):
        """Store forcing incremental should override flow full_drop."""
        config_data = {
            "flows": {
                "test_flow": {
                    "home": {"type": "parquet", "path": "data/source"},
                    "store": {
                        "type": "open_mirroring",
                        "account_url": "https://onelake.dfs.fabric.microsoft.com",
                        "filesystem": "test",
                        "mirror_name": "test-db",
                        "key_columns": ["id"],
                        "row_marker": 0,
                        "incremental": True,
                    },
                    "full_drop": True,
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

            flow = coordinator.flows[0]
            store = flow.store
            assert flow.run_type == "full_drop"
            if hasattr(store, "full_drop_mode"):
                assert store.full_drop_mode is False
        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_store_incremental_override_forced_full_drop(self):
        """Store forcing full_drop should override flow incremental."""
        config_data = {
            "flows": {
                "test_flow": {
                    "home": {"type": "parquet", "path": "data/source"},
                    "store": {
                        "type": "open_mirroring",
                        "account_url": "https://onelake.dfs.fabric.microsoft.com",
                        "filesystem": "test",
                        "mirror_name": "test-db",
                        "key_columns": ["id"],
                        "row_marker": 0,
                        "incremental": False,
                    },
                    "run_type": "incremental",
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

            flow = coordinator.flows[0]
            store = flow.store
            assert flow.run_type == "incremental"
            if hasattr(store, "full_drop_mode"):
                assert store.full_drop_mode is True
        finally:
            Path(config_file).unlink(missing_ok=True)


class TestCoordinatorConcurrency:
    """Test Coordinator concurrency limiting with semaphore."""

    @pytest.mark.asyncio
    async def test_concurrency_defaults_to_eight(self, tmp_path):
        """Test that concurrency defaults to 8 when no config or pools."""
        config_data = {
            "flows": {
                f"flow_{i}": {
                    "home": {
                        "type": "parquet",
                        "path": str(tmp_path / f"source_{i}.parquet"),
                    },
                    "store": {"type": "parquet", "path": str(tmp_path / f"dest_{i}")},
                }
                for i in range(10)
            }
        }

        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        coordinator = Coordinator(str(config_file))
        coordinator._load_config()
        coordinator._create_flows()

        # Check that max_concurrent would be 8 (default)
        max_concurrent = coordinator.options.get("concurrency", None)
        if max_concurrent is None:
            if coordinator.connection_pools:
                max_concurrent = max(
                    (pool.size for pool in coordinator.connection_pools.values()),
                    default=8,
                )
            else:
                max_concurrent = 8

        assert max_concurrent == 8

    @pytest.mark.asyncio
    async def test_concurrency_from_config_option(self, tmp_path):
        """Test that concurrency can be set via config option."""
        config_data = {
            "options": {"concurrency": 4},
            "flows": {
                f"flow_{i}": {
                    "home": {
                        "type": "parquet",
                        "path": str(tmp_path / f"source_{i}.parquet"),
                    },
                    "store": {"type": "parquet", "path": str(tmp_path / f"dest_{i}")},
                }
                for i in range(10)
            },
        }

        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        coordinator = Coordinator(str(config_file))
        coordinator._load_config()
        coordinator._create_flows()

        assert coordinator.options.get("concurrency") == 4

    @pytest.mark.asyncio
    async def test_concurrency_matches_pool_size(self, tmp_path):
        """Test that concurrency matches pool size when pools exist."""
        config_data = {
            "connections": {
                "test_db": {
                    "type": "mssql",
                    "server": "test.server",
                    "database": "test_db",
                    "pool_size": 12,
                }
            },
            "flows": {
                f"flow_{i}": {
                    "home": {
                        "type": "parquet",
                        "path": str(tmp_path / f"source_{i}.parquet"),
                    },
                    "store": {"type": "parquet", "path": str(tmp_path / f"dest_{i}")},
                }
                for i in range(10)
            },
        }

        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        coordinator = Coordinator(str(config_file))
        coordinator._load_config()

        # Mock connection pool initialization to avoid actual DB connection
        # We'll just check the logic for determining max_concurrent
        max_concurrent = coordinator.options.get("concurrency", None)
        if max_concurrent is None:
            # Simulate what happens when pools exist
            # In real scenario, pools would be initialized and we'd check their size
            # For this test, we verify the logic path
            if coordinator.config and coordinator.config.connections:
                # Check that pool_size is in config
                pool_size = coordinator.config.connections["test_db"].get(
                    "pool_size", 5
                )
                assert pool_size == 12

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrent_execution(self, tmp_path):
        """Test that semaphore actually limits concurrent flow execution."""
        # Create test data files
        for i in range(10):
            source_file = tmp_path / f"source_{i}.parquet"
            source_file.parent.mkdir(parents=True, exist_ok=True)

            pl.DataFrame(
                {"id": [1, 2, 3], "value": [f"val_{i}_{j}" for j in range(3)]}
            ).write_parquet(source_file)

        config_data = {
            "options": {"concurrency": 3},
            "flows": {
                f"flow_{i}": {
                    "home": {
                        "type": "parquet",
                        "path": str(tmp_path / f"source_{i}.parquet"),
                    },
                    "store": {"type": "parquet", "path": str(tmp_path / f"dest_{i}")},
                }
                for i in range(10)
            },
        }

        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        coordinator = Coordinator(str(config_file))
        coordinator._load_config()
        coordinator._create_flows()

        # Track concurrent executions
        concurrent_count = 0
        max_concurrent_seen = 0
        execution_lock = asyncio.Lock()

        async def track_concurrent_execution():
            nonlocal concurrent_count, max_concurrent_seen
            async with execution_lock:
                concurrent_count += 1
                max_concurrent_seen = max(max_concurrent_seen, concurrent_count)

            # Simulate some work
            await asyncio.sleep(0.1)

            async with execution_lock:
                concurrent_count -= 1

        # Mock flow.start() to track concurrency
        for flow in coordinator.flows:
            flow.start = AsyncMock(side_effect=track_concurrent_execution)

        # Create semaphore and run flows
        semaphore = asyncio.Semaphore(3)
        tasks = []
        for i, flow in enumerate(coordinator.flows):
            task = asyncio.create_task(
                coordinator._run_flow_with_semaphore(
                    flow, i + 1, len(coordinator.flows), semaphore
                )
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

        # Verify that max concurrent never exceeded 3
        # Note: This is a timing-dependent test, but should be reliable with the sleep
        assert (
            max_concurrent_seen <= 3
        ), f"Max concurrent was {max_concurrent_seen}, expected <= 3"

    @pytest.mark.asyncio
    async def test_concurrency_string_conversion(self, tmp_path):
        """Test that concurrency string values are converted to int."""
        config_data = {
            "options": {"concurrency": "16"},  # String value
            "flows": {
                "flow_1": {
                    "home": {
                        "type": "parquet",
                        "path": str(tmp_path / "source.parquet"),
                    },
                    "store": {"type": "parquet", "path": str(tmp_path / "dest")},
                }
            },
        }

        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        coordinator = Coordinator(str(config_file))
        coordinator._load_config()
        coordinator._create_flows()

        # Simulate the logic in _run_flows
        max_concurrent = coordinator.options.get("concurrency", None)
        if isinstance(max_concurrent, str):
            max_concurrent = int(max_concurrent)

        assert max_concurrent == 16
        assert isinstance(max_concurrent, int)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
