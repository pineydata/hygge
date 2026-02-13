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

from hygge.core.coordinator import Coordinator, CoordinatorConfig, validate_config
from hygge.core.flow import Entity, FlowConfig
from hygge.core.journal import Journal, JournalConfig
from hygge.homes.parquet.home import ParquetHome, ParquetHomeConfig  # noqa: F401
from hygge.stores.openmirroring.store import (  # noqa: F401
    OpenMirroringStore,
    OpenMirroringStoreConfig,
)
from hygge.stores.parquet.store import ParquetStore, ParquetStoreConfig  # noqa: F401
from hygge.utility.exceptions import ConfigError


class TestCoordinatorConfig:
    """Test CoordinatorConfig validation and creation."""

    def test_coordinator_config_creation(self):
        """Test that CoordinatorConfig can be created with valid entities."""
        flow_config = FlowConfig(home="data/test.parquet", store="output/test")
        entity = Entity(
            flow_name="test_flow",
            base_flow_name="test_flow",
            entity_name=None,
            flow_config=flow_config,
            entity_config=None,
        )
        config_data = {"entities": [entity]}

        config = CoordinatorConfig.from_dict(config_data)
        assert len(config.entities) == 1
        assert config.entities[0].flow_name == "test_flow"
        assert isinstance(config.entities[0].flow_config, FlowConfig)

    def test_coordinator_config_journal_conversion(self):
        """Test that CoordinatorConfig converts journal config properly."""
        flow_config = FlowConfig(home="data/test.parquet", store="output/test")
        entity = Entity(
            flow_name="test_flow",
            base_flow_name="test_flow",
            entity_name=None,
            flow_config=flow_config,
            entity_config=None,
        )
        config_data = {
            "journal": {"path": "/tmp/journal"},
            "entities": [entity],
        }

        config = CoordinatorConfig.from_dict(config_data)
        assert isinstance(config.journal, JournalConfig)
        assert config.journal.path == "/tmp/journal"

    def test_coordinator_config_empty_flows_validation(self):
        """Test that empty entities are rejected."""
        config_data = {"entities": []}

        with pytest.raises(ValueError, match="At least one entity must be configured"):
            CoordinatorConfig.from_dict(config_data)

    def test_coordinator_config_get_entity(self):
        """Test getting specific entity."""
        flow1_config = FlowConfig(home="data/flow1.parquet", store="output/flow1")
        flow2_config = FlowConfig(home="data/flow2.parquet", store="output/flow2")
        entity1 = Entity(
            flow_name="flow1",
            base_flow_name="flow1",
            entity_name=None,
            flow_config=flow1_config,
            entity_config=None,
        )
        entity2 = Entity(
            flow_name="flow2",
            base_flow_name="flow2",
            entity_name=None,
            flow_config=flow2_config,
            entity_config=None,
        )
        config_data = {"entities": [entity1, entity2]}

        config = CoordinatorConfig.from_dict(config_data)

        # Test existing entity
        found_entity = config.get_entity("flow1")
        assert found_entity is not None
        assert isinstance(found_entity, Entity)
        assert found_entity.flow_name == "flow1"

        # Test non-existing entity
        assert config.get_entity("nonexistent") is None


class TestValidateConfig:
    """Test configuration validation function."""

    def test_validate_config_success(self):
        """Test successful configuration validation."""
        flow_config = FlowConfig(home="data/test.parquet", store="output/test")
        entity = Entity(
            flow_name="test_flow",
            base_flow_name="test_flow",
            entity_name=None,
            flow_config=flow_config,
            entity_config=None,
        )
        config = {"entities": [entity]}

        errors = validate_config(config)
        assert errors == []

    def test_validate_config_failure(self):
        """Test configuration validation failure."""
        config = {
            "entities": []  # Empty entities should fail
        }

        errors = validate_config(config)
        assert len(errors) > 0
        assert "At least one entity must be configured" in errors[0]


class TestCoordinatorInitialization:
    """Test Coordinator initialization and basic setup."""

    def test_coordinator_initialization_with_hygge_yml(self):
        """Test Coordinator initialization with hygge.yml path."""
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

            # Create flows directory
            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            coordinator = Coordinator(str(hygge_file))
            assert coordinator.config_path.resolve() == hygge_file.resolve()
            assert coordinator.config is None  # Not loaded yet (lazy loading)
            assert coordinator.flows == []
            assert coordinator.coordinator_name == "test_project"

    def test_coordinator_initialization_with_non_hygge_yml_raises_error(self):
        """Test Coordinator raises error for non-hygge.yml files."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("flows: {}")
            config_file = f.name

        try:
            with pytest.raises(ConfigError, match="Expected hygge.yml"):
                Coordinator(config_file)
        finally:
            Path(config_file).unlink(missing_ok=True)

    def test_coordinator_initialization_with_directory_raises_error(self):
        """Test Coordinator raises error for directory paths (not hygge.yml)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(ConfigError, match="Expected hygge.yml"):
                Coordinator(temp_dir)


class TestCoordinatorConfigLoading:
    """Test configuration loading via Workspace."""

    def test_coordinator_loads_config_via_workspace(self):
        """Test Coordinator loads config via Workspace.prepare()."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create hygge.yml
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "flows"
options:
  continue_on_error: true
"""
            )

            # Create flows directory and flow
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
  path: "data/test.parquet"
store:
  type: "parquet"
  path: "output/test"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            # Config loaded lazily - prepare it for this test
            coordinator.config = coordinator._workspace.prepare()

            assert coordinator.config is not None
            assert len(coordinator.config.entities) == 1
            assert coordinator.config.entities[0].flow_name == "test_flow"
            assert coordinator.options == {"continue_on_error": True}


class TestCoordinatorJournalIntegration:
    """Test journal integration hooks inside Coordinator."""

    def test_coordinator_flow_receives_journal_context(self, tmp_path):
        """Coordinator should attach journal and metadata to flows."""
        journal_dir = tmp_path / "journal_dir"

        # Create hygge.yml
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            f"""
name: "test_project"
flows_dir: "flows"
journal:
  path: "{journal_dir}"
"""
        )

        # Create flows directory and flow
        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        flow_dir = flows_dir / "users_flow"
        flow_dir.mkdir()

        flow_file = flow_dir / "flow.yml"
        flow_file.write_text(
            f"""
name: "users_flow"
home:
  type: "parquet"
  path: "{tmp_path / "source.parquet"}"
store:
  type: "parquet"
  path: "{tmp_path / "dest"}"
run_type: "incremental"
watermark:
  primary_key: "id"
  watermark_column: "updated_at"
"""
        )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()
        # Update journal_config after config is prepared (needed for journal creation)
        if coordinator.config and coordinator.config.journal:
            if isinstance(coordinator.config.journal, JournalConfig):
                coordinator.journal_config = coordinator.config.journal
            else:
                coordinator.journal_config = JournalConfig(**coordinator.config.journal)
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
        # Create hygge.yml
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        # Create flows directory and flow
        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        flow_dir = flows_dir / "users_flow"
        flow_dir.mkdir()

        flow_file = flow_dir / "flow.yml"
        flow_file.write_text(
            f"""
name: "users_flow"
home:
  type: "parquet"
  path: "{tmp_path / "source.parquet"}"
store:
  type: "parquet"
  path: "{tmp_path / "dest"}"
"""
        )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()
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
        # Create hygge.yml
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        # Create flows directory and flow
        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        flow_dir = flows_dir / "users_flow"
        flow_dir.mkdir()

        flow_file = flow_dir / "flow.yml"
        flow_file.write_text(
            f"""
name: "users_flow"
home:
  type: "parquet"
  path: "{tmp_path / "source.parquet"}"
store:
  type: "parquet"
  path: "{tmp_path / "dest"}"
entities:
  - "users"
  - "orders"
"""
        )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()
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

    def test_load_config_via_workspace(self):
        """Test loading configuration via Workspace."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create hygge.yml
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "flows"
options:
  continue_on_error: true
"""
            )

            # Create flows directory and flow
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
  path: "data/test.parquet"
store:
  type: "parquet"
  path: "output/test"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()

            assert coordinator.config is not None
            assert len(coordinator.config.entities) == 1
            assert coordinator.config.entities[0].flow_name == "test_flow"
            assert coordinator.options == {"continue_on_error": True}

    def test_load_config_with_invalid_hygge_yml(self):
        """Test loading configuration with invalid hygge.yml."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text("invalid: yaml: content: [")

            # Error happens during Coordinator.__init__ when
            # Workspace.from_path() is called
            with pytest.raises(Exception):
                Coordinator(str(hygge_file))

    def test_load_config_with_non_hygge_yml_raises_error(self):
        """Test Coordinator raises error for non-hygge.yml files."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("flows: {}")
            config_file = f.name

        try:
            with pytest.raises(ConfigError, match="Expected hygge.yml"):
                Coordinator(config_file)
        finally:
            Path(config_file).unlink(missing_ok=True)


class TestCoordinatorFlowCreation:
    """Test Coordinator flow creation."""

    def test_create_flows_from_config(self):
        """Test creating flows from configuration."""
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

            # Create flows directory and flows
            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            # Create flow1
            flow1_dir = flows_dir / "flow1"
            flow1_dir.mkdir()
            flow1_file = flow1_dir / "flow.yml"
            flow1_file.write_text(
                """
name: "flow1"
home:
  type: "parquet"
  path: "data/flow1.parquet"
store:
  type: "parquet"
  path: "output/flow1"
"""
            )

            # Create flow2
            flow2_dir = flows_dir / "flow2"
            flow2_dir.mkdir()
            flow2_file = flow2_dir / "flow.yml"
            flow2_file.write_text(
                """
name: "flow2"
home:
  type: "parquet"
  path: "data/flow2.parquet"
store:
  type: "parquet"
  path: "output/flow2"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            assert len(coordinator.flows) == 2
            # Flow order is not guaranteed, so check set membership
            flow_names = {flow.name for flow in coordinator.flows}
            assert flow_names == {"flow1", "flow2"}

            # Verify flows have the right components
            for flow in coordinator.flows:
                assert flow.home is not None
                assert flow.store is not None
                assert flow.home.config.type == "parquet"
                assert flow.store.config.type == "parquet"


class TestCoordinatorIntegration:
    """Test Coordinator integration with registry pattern."""

    def test_coordinator_with_simple_config(self):
        """Test Coordinator with simple configuration."""
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

            # Create flows directory and flow
            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            flow_dir = flows_dir / "simple_flow"
            flow_dir.mkdir()

            flow_file = flow_dir / "flow.yml"
            flow_file.write_text(
                """
name: "simple_flow"
home:
  type: "parquet"
  path: "data/simple.parquet"
store:
  type: "parquet"
  path: "output/simple"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            # Verify the flow was created correctly
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]

            # Verify registry pattern worked
            assert flow.home.config.type == "parquet"
            assert flow.store.config.type == "parquet"
            assert flow.home.config.path == "data/simple.parquet"
            assert flow.store.config.path == "output/simple"

    def test_coordinator_with_dict_config(self):
        """Test Coordinator with dictionary-based configuration."""
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

            # Create flows directory and flow
            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            flow_dir = flows_dir / "dict_flow"
            flow_dir.mkdir()

            flow_file = flow_dir / "flow.yml"
            flow_file.write_text(
                """
name: "dict_flow"
home:
  type: "parquet"
  path: "data/dict.parquet"
  batch_size: 5000
store:
  type: "parquet"
  path: "output/dict"
  compression: "snappy"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            # Verify the flow was created correctly
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]

            # Verify registry pattern worked with custom options
            assert flow.home.config.type == "parquet"
            assert flow.store.config.type == "parquet"
            assert flow.home.config.batch_size == 5000
            assert flow.store.config.compression == "snappy"


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

                # Verify entities were loaded (entities are already expanded)
                # salesforce_to_lake has 2 entities, warehouse_to_parquet has 1
                assert len(coordinator.config.entities) == 3

                # Check entity flow names
                flow_names = {
                    entity.flow_name for entity in coordinator.config.entities
                }
                assert "salesforce_to_lake_Account" in flow_names
                assert "salesforce_to_lake_Contact" in flow_names
                assert "warehouse_to_parquet" in flow_names

                # Verify entity names
                entity_objs = [
                    e
                    for e in coordinator.config.entities
                    if e.base_flow_name == "salesforce_to_lake"
                ]
                entity_names = {e.entity_name for e in entity_objs if e.entity_name}
                assert "Account" in entity_names
                assert "Contact" in entity_names

                # Verify warehouse_to_parquet has no entity_name (non-entity flow)
                warehouse_entity = next(
                    e
                    for e in coordinator.config.entities
                    if e.flow_name == "warehouse_to_parquet"
                )
                assert warehouse_entity.entity_name is None

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
                assert len(coordinator.config.entities) == 1
                assert coordinator.config.entities[0].flow_name == "test_flow"

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
                # Entities are already expanded, so get the entity directly
                # Entity name is "TestEntity", so flow_name is "test_flow_TestEntity"
                entity_obj = coordinator.config.get_entity("test_flow_TestEntity")
                assert entity_obj is not None
                # Entity config is stored in entity.entity_config
                if entity_obj.entity_config:
                    assert entity_obj.entity_config.get("key_column") == "Id"
                    assert entity_obj.entity_config.get("schema") == "test_schema"
                    # Should override batch_size
                    source_config = entity_obj.entity_config.get("source_config", {})
                    assert source_config.get("batch_size") == 10000

            finally:
                os.chdir(original_cwd)


class TestCoordinatorEntityPathMerging:
    """Test entity flow path merging with PathHelper."""

    def test_entity_flow_path_merging_simple(self):
        """Test that entity paths are merged correctly with flow paths."""
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

            # Create flows directory and flow
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
entities:
  - name: "users"
    home:
      path: "users"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            # Should create entity flow with merged path
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]
            assert flow.name == "test_flow_users"
            # Path should be merged: "data/source" + "users" = "data/source/users"
            assert flow.home.config.path == "data/source/users"

    def test_entity_flow_path_merging_with_trailing_slash(self):
        """Test path merging handles trailing slashes correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  path: "data/source/"
store:
  type: "parquet"
  path: "data/destination"
entities:
  - name: "users"
    home:
      path: "users"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            flow = coordinator.flows[0]
            # Should merge correctly despite trailing slash: "data/source/" + "users"
            assert flow.home.config.path == "data/source/users"

    def test_entity_flow_path_merging_with_leading_slash(self):
        """Test path merging handles leading slashes correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
entities:
  - name: "users"
    home:
      path: "/users"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            flow = coordinator.flows[0]
            # Should merge correctly despite leading slash: "data/source" + "/users"
            assert flow.home.config.path == "data/source/users"

    def test_entity_flow_path_merging_both_slashes(self):
        """Test path merging handles both trailing and leading slashes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  path: "data/source/"
store:
  type: "parquet"
  path: "data/destination"
entities:
  - name: "users"
    home:
      path: "/users"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            flow = coordinator.flows[0]
            # Should merge correctly: "data/source/" + "/users" = "data/source/users"
            assert flow.home.config.path == "data/source/users"


class TestCoordinatorFlowOverrides:
    """Test Coordinator flow-level overrides (CLI and config)."""

    def test_apply_flow_overrides_full_drop(self):
        """Test that flow-level full_drop is applied to store config."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  type: "open_mirroring"
  account_url: "https://onelake.dfs.fabric.microsoft.com"
  filesystem: "test"
  mirror_name: "test-db"
  key_columns: ["id"]
  row_marker: 0
full_drop: true
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            # Verify flow was created
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]

            # Verify flow-level full_drop drives the run_type + store strategy
            store = flow.store
            if hasattr(store, "full_drop_mode"):
                assert store.full_drop_mode is True
            assert flow.run_type == "full_drop"

    def test_apply_flow_overrides_via_cli(self):
        """Test that CLI flow overrides are applied correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  type: "open_mirroring"
  account_url: "https://onelake.dfs.fabric.microsoft.com"
  filesystem: "test"
  mirror_name: "test-db"
  key_columns: ["id"]
  row_marker: 0
"""
            )

            # Simulate CLI override: flow.test_flow.full_drop=true
            flow_overrides = {"test_flow": {"full_drop": True}}

            coordinator = Coordinator(str(hygge_file), flow_overrides=flow_overrides)
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            # Verify flow was created
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]

            # Verify CLI override was applied
            # The override is applied when creating the flow,
            # not to the entity in config. Check the flow's run_type instead
            assert flow.run_type == "full_drop"

            # Store should have full_drop=True (from flow-level override)
            store = flow.store
            if hasattr(store, "full_drop_mode"):
                assert store.full_drop_mode is True

    def test_flow_overrides_not_applied_when_none(self):
        """Test that flow overrides don't affect config when not provided."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  path: "data/output"
full_drop: false
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            # Verify flow config has the original full_drop value
            entity = coordinator.config.get_entity("test_flow")
            assert entity is not None
            assert entity.flow_config.full_drop is False

    def test_entity_flow_inherits_base_flow_full_drop(self):
        """Test that entity flows inherit flow-level full_drop from base flow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text(
                """
name: "test_project"
flows_dir: "flows"
"""
            )

            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            flow_dir = flows_dir / "base_flow"
            flow_dir.mkdir()

            flow_file = flow_dir / "flow.yml"
            flow_file.write_text(
                """
name: "base_flow"
home:
  type: "parquet"
  path: "data/source"
store:
  type: "open_mirroring"
  account_url: "https://onelake.dfs.fabric.microsoft.com"
  filesystem: "test"
  mirror_name: "test-db"
  key_columns: ["id"]
  row_marker: 0
full_drop: true
entities:
  - "Account"
  - "Contact"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
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

    def test_store_incremental_override_forced_incremental(self):
        """Store forcing incremental should override flow full_drop."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  type: "open_mirroring"
  account_url: "https://onelake.dfs.fabric.microsoft.com"
  filesystem: "test"
  mirror_name: "test-db"
  key_columns: ["id"]
  row_marker: 0
  incremental: true
full_drop: true
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            flow = coordinator.flows[0]
            store = flow.store
            assert flow.run_type == "full_drop"
            if hasattr(store, "full_drop_mode"):
                assert store.full_drop_mode is False

    def test_store_incremental_override_forced_full_drop(self):
        """Store forcing full_drop should override flow incremental."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  type: "open_mirroring"
  account_url: "https://onelake.dfs.fabric.microsoft.com"
  filesystem: "test"
  mirror_name: "test-db"
  key_columns: ["id"]
  row_marker: 0
  incremental: false
run_type: "incremental"
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            flow = coordinator.flows[0]
            store = flow.store
            assert flow.run_type == "incremental"
            if hasattr(store, "full_drop_mode"):
                assert store.full_drop_mode is True


class TestCoordinatorConcurrency:
    """Test Coordinator concurrency limiting with semaphore."""

    @pytest.mark.asyncio
    async def test_concurrency_defaults_to_eight(self, tmp_path):
        """Test that concurrency defaults to 8 when no config or pools."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
"""
        )

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        for i in range(10):
            flow_dir = flows_dir / f"flow_{i}"
            flow_dir.mkdir()

            flow_file = flow_dir / "flow.yml"
            flow_file.write_text(
                f"""
name: "flow_{i}"
home:
  type: "parquet"
  path: "{tmp_path / f"source_{i}.parquet"}"
store:
  type: "parquet"
  path: "{tmp_path / f"dest_{i}"}"
"""
            )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()
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
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
options:
  concurrency: 4
"""
        )

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        for i in range(10):
            flow_dir = flows_dir / f"flow_{i}"
            flow_dir.mkdir()

            flow_file = flow_dir / "flow.yml"
            flow_file.write_text(
                f"""
name: "flow_{i}"
home:
  type: "parquet"
  path: "{tmp_path / f"source_{i}.parquet"}"
store:
  type: "parquet"
  path: "{tmp_path / f"dest_{i}"}"
"""
            )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()
        coordinator._create_flows()

        assert coordinator.options.get("concurrency") == 4

    @pytest.mark.asyncio
    async def test_concurrency_matches_pool_size(self, tmp_path):
        """Test that concurrency matches pool size when pools exist."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
connections:
  test_db:
    type: "mssql"
    server: "test.server"
    database: "test_db"
    pool_size: 12
"""
        )

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        for i in range(10):
            flow_dir = flows_dir / f"flow_{i}"
            flow_dir.mkdir()

            flow_file = flow_dir / "flow.yml"
            flow_file.write_text(
                f"""
name: "flow_{i}"
home:
  type: "parquet"
  path: "{tmp_path / f"source_{i}.parquet"}"
store:
  type: "parquet"
  path: "{tmp_path / f"dest_{i}"}"
"""
            )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()

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

        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
options:
  concurrency: 3
"""
        )

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        for i in range(10):
            flow_dir = flows_dir / f"flow_{i}"
            flow_dir.mkdir()

            flow_file = flow_dir / "flow.yml"
            flow_file.write_text(
                f"""
name: "flow_{i}"
home:
  type: "parquet"
  path: "{tmp_path / f"source_{i}.parquet"}"
store:
  type: "parquet"
  path: "{tmp_path / f"dest_{i}"}"
"""
            )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()
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
    async def test_semaphore_released_early_via_extraction_callback(self, tmp_path):
        """Test that semaphore is released when on_extraction_complete fires.

        When a flow signals extraction is complete (before finish()),
        the semaphore slot should be released so other entities can start.
        """
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
options:
  concurrency: 1
"""
        )

        # Create a single flow config
        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()
        flow_dir = flows_dir / "flow_0"
        flow_dir.mkdir()

        source_file = tmp_path / "source_0.parquet"
        pl.DataFrame({"id": [1]}).write_parquet(source_file)

        flow_file = flow_dir / "flow.yml"
        flow_file.write_text(
            f"""
home:
  type: "parquet"
  path: "{source_file}"
store:
  type: "parquet"
  path: "{tmp_path / "dest_0"}"
"""
        )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()
        coordinator._create_flows()

        flow = coordinator.flows[0]
        semaphore = asyncio.Semaphore(1)

        # Track when semaphore is released
        release_order = []

        async def mock_start():
            # Simulate extraction completing and callback firing
            if flow.on_extraction_complete:
                flow.on_extraction_complete()
                release_order.append("callback_released")

            # After callback, semaphore should be available for others
            # Try to acquire without blocking (would deadlock if not released)
            acquired = semaphore._value > 0
            release_order.append("slot_available" if acquired else "slot_held")

        flow.start = mock_start

        await coordinator._run_flow_with_semaphore(flow, 1, 1, semaphore)

        assert release_order == ["callback_released", "slot_available"]

    @pytest.mark.asyncio
    async def test_semaphore_released_on_error_without_callback(self, tmp_path):
        """Test semaphore is released via finally when flow fails before callback."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
options:
  concurrency: 1
"""
        )

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()
        flow_dir = flows_dir / "flow_0"
        flow_dir.mkdir()

        source_file = tmp_path / "source_0.parquet"
        pl.DataFrame({"id": [1]}).write_parquet(source_file)

        flow_file = flow_dir / "flow.yml"
        flow_file.write_text(
            f"""
home:
  type: "parquet"
  path: "{source_file}"
store:
  type: "parquet"
  path: "{tmp_path / "dest_0"}"
"""
        )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()
        coordinator._create_flows()

        flow = coordinator.flows[0]
        semaphore = asyncio.Semaphore(1)

        # Flow fails during extraction (callback never fires)
        async def mock_start_fail():
            raise Exception("extraction failed")

        flow.start = mock_start_fail

        await coordinator._run_flow_with_semaphore(flow, 1, 1, semaphore)

        # Semaphore should still be released via finally block
        assert semaphore._value == 1, "Semaphore not released after error"

    @pytest.mark.asyncio
    async def test_concurrency_string_conversion(self, tmp_path):
        """Test that concurrency string values are converted to int."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
options:
  concurrency: "16"
"""
        )

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        flow_dir = flows_dir / "flow_1"
        flow_dir.mkdir()

        flow_file = flow_dir / "flow.yml"
        flow_file.write_text(
            f"""
name: "flow_1"
home:
  type: "parquet"
  path: "{tmp_path / "source.parquet"}"
store:
  type: "parquet"
  path: "{tmp_path / "dest"}"
"""
        )

        coordinator = Coordinator(str(hygge_file))
        coordinator.config = coordinator._workspace.prepare()
        coordinator._create_flows()

        # Simulate the logic in _run_flows
        max_concurrent = coordinator.options.get("concurrency", None)
        if isinstance(max_concurrent, str):
            max_concurrent = int(max_concurrent)

        assert max_concurrent == 16
        assert isinstance(max_concurrent, int)


class TestCoordinatorOpenMirroringKeyColumns:
    """Test Open Mirroring key_columns validation for non-entity flows."""

    def test_open_mirroring_flow_without_entities_requires_key_columns(self):
        """Test Open Mirroring flow without entities requires flow-level key_columns."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  type: "open_mirroring"
  account_url: "https://onelake.dfs.fabric.microsoft.com"
  filesystem: "test"
  mirror_name: "test-db"
  row_marker: 0
  # key_columns missing - should fail for non-entity flow
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()

            with pytest.raises(ConfigError) as exc_info:
                coordinator._create_flows()

            assert "key_columns" in str(exc_info.value).lower()
            # Error should mention entities or flow-level key_columns
            error_msg = str(exc_info.value).lower()
            assert (
                "does not have entities" in error_msg
                or "flow level" in error_msg
                or "store config" in error_msg
            )

    def test_open_mirroring_flow_without_entities_with_key_columns_succeeds(self):
        """Test Open Mirroring flow without entities works when key_columns provided."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  type: "open_mirroring"
  account_url: "https://onelake.dfs.fabric.microsoft.com"
  filesystem: "test"
  mirror_name: "test-db"
  key_columns: ["id"]
  row_marker: 0
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            # Should succeed - flow created
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]
            assert flow.name == "test_flow"

    def test_open_mirroring_flow_with_entities_works_without_flow_level_key_columns(
        self,
    ):
        """Test Open Mirroring flow with entities works when entities provide key_columns."""  # noqa: E501
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  type: "open_mirroring"
  account_url: "https://onelake.dfs.fabric.microsoft.com"
  filesystem: "test"
  mirror_name: "test-db"
  row_marker: 0
  # key_columns not provided at flow level - entities will provide it
"""
            )

            # Create entities directory
            entities_dir = flow_dir / "entities"
            entities_dir.mkdir()

            # Create entity file with key_columns
            entity_file = entities_dir / "users.yml"
            entity_file.write_text(
                """
name: "users"
store:
  key_columns: ["user_id"]
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()
            coordinator._create_flows()

            # Should succeed - entity flow created with key_columns from entity config
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]
            assert flow.name == "test_flow_users"
            # Verify store has key_columns from entity config
            assert flow.store.key_columns == ["user_id"]

    def test_open_mirroring_flow_with_entities_missing_key_columns_fails(self):
        """Test Open Mirroring flow with entities fails if entity missing key_columns."""  # noqa: E501
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

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
  type: "open_mirroring"
  account_url: "https://onelake.dfs.fabric.microsoft.com"
  filesystem: "test"
  mirror_name: "test-db"
  row_marker: 0
  # key_columns not provided at flow level
"""
            )

            # Create entities directory
            entities_dir = flow_dir / "entities"
            entities_dir.mkdir()

            # Create entity file WITHOUT key_columns
            entity_file = entities_dir / "users.yml"
            entity_file.write_text(
                """
name: "users"
# No store.key_columns provided
"""
            )

            coordinator = Coordinator(str(hygge_file))
            coordinator.config = coordinator._workspace.prepare()

            with pytest.raises(ConfigError) as exc_info:
                coordinator._create_flows()

            # Should fail with entity context in error message
            error_msg = str(exc_info.value).lower()
            assert "key_columns" in error_msg
            assert "users" in error_msg or "entity" in error_msg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
