"""
Unit tests for Workspace class.

Tests workspace discovery, configuration loading, and flow preparation.
Following hygge's testing philosophy: test real behavior with actual configurations.
"""
import os
from pathlib import Path

import pytest

from hygge.core.coordinator import CoordinatorConfig
from hygge.core.flow import FlowConfig
from hygge.core.workspace import Workspace
from hygge.homes.parquet.home import ParquetHome, ParquetHomeConfig  # noqa: F401
from hygge.stores.parquet.store import ParquetStore, ParquetStoreConfig  # noqa: F401
from hygge.utility.exceptions import ConfigError


class TestWorkspaceFind:
    """Test Workspace.find() - finding hygge.yml by walking up directories."""

    def test_find_hygge_yml_in_current_directory(self, tmp_path):
        """Test finding hygge.yml in current directory."""
        # Create hygge.yml in temp directory
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        original_cwd = Path.cwd()
        os.chdir(tmp_path)

        try:
            workspace = Workspace.find()
            assert workspace.hygge_yml == hygge_file
            assert workspace.name == "test_project"
            assert workspace.flows_dir == "flows"
        finally:
            os.chdir(original_cwd)

    def test_find_hygge_yml_in_parent_directory(self, tmp_path):
        """Test finding hygge.yml in parent directory."""
        # Create hygge.yml in temp directory
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        # Create subdirectory
        subdir = tmp_path / "subdir" / "nested"
        subdir.mkdir(parents=True)

        original_cwd = Path.cwd()
        os.chdir(subdir)

        try:
            workspace = Workspace.find()
            assert workspace.hygge_yml == hygge_file
            assert workspace.name == "test_project"
        finally:
            os.chdir(original_cwd)

    def test_find_hygge_yml_with_custom_start_path(self, tmp_path):
        """Test finding hygge.yml with custom start_path."""
        # Create hygge.yml in temp directory
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        # Create subdirectory
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        workspace = Workspace.find(start_path=subdir)
        assert workspace.hygge_yml == hygge_file
        assert workspace.name == "test_project"

    def test_find_hygge_yml_raises_error_when_not_found(self, tmp_path):
        """Test that find() raises error when hygge.yml not found."""
        original_cwd = Path.cwd()
        os.chdir(tmp_path)

        try:
            with pytest.raises(ConfigError, match="No hygge.yml found"):
                Workspace.find()
        finally:
            os.chdir(original_cwd)

    def test_find_hygge_yml_searches_multiple_parents(self, tmp_path):
        """Test that find() searches multiple parent directories."""
        # Create hygge.yml in temp directory
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        # Create deeply nested subdirectory
        nested = tmp_path / "level1" / "level2" / "level3" / "level4"
        nested.mkdir(parents=True)

        original_cwd = Path.cwd()
        os.chdir(nested)

        try:
            workspace = Workspace.find()
            assert workspace.hygge_yml == hygge_file
        finally:
            os.chdir(original_cwd)


class TestWorkspaceFromPath:
    """Test Workspace.from_path() - creating Workspace from hygge.yml path."""

    def test_from_path_reads_workspace_name(self, tmp_path):
        """Test that from_path() reads workspace name from hygge.yml."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "my_project"\nflows_dir: "flows"')

        workspace = Workspace.from_path(hygge_file)
        assert workspace.name == "my_project"
        assert workspace.flows_dir == "flows"
        assert workspace.hygge_yml == hygge_file
        assert workspace.root == tmp_path

    def test_from_path_uses_directory_name_as_default(self, tmp_path):
        """Test that from_path() uses directory name as default name."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('flows_dir: "flows"')

        workspace = Workspace.from_path(hygge_file)
        assert workspace.name == tmp_path.name

    def test_from_path_uses_default_flows_dir(self, tmp_path):
        """Test that from_path() uses default flows_dir when not specified."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"')

        workspace = Workspace.from_path(hygge_file)
        assert workspace.flows_dir == "flows"

    def test_from_path_handles_empty_yaml(self, tmp_path):
        """Test that from_path() handles empty hygge.yml."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text("")

        workspace = Workspace.from_path(hygge_file)
        assert workspace.name == tmp_path.name
        assert workspace.flows_dir == "flows"


class TestWorkspaceReadConfig:
    """Test Workspace._read_workspace_config() - reading hygge.yml."""

    def test_read_workspace_config_loads_basic_config(self, tmp_path):
        """Test reading basic workspace configuration."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
connections:
  db1:
    type: "mssql"
    server: "localhost"
options:
  log_level: "DEBUG"
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        workspace._read_workspace_config()

        assert workspace.config["name"] == "test_project"
        assert workspace.config["flows_dir"] == "flows"
        assert "connections" in workspace.config
        assert "options" in workspace.config
        assert workspace.connections["db1"]["type"] == "mssql"
        assert workspace.options["log_level"] == "DEBUG"

    def test_read_workspace_config_expands_env_vars(self, tmp_path, monkeypatch):
        """Test that workspace config expands environment variables."""
        monkeypatch.setenv("TEST_SERVER", "prod.example.com")
        monkeypatch.setenv("TEST_DB", "production")

        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
connections:
  db1:
    server: "${TEST_SERVER}"
    database: "${TEST_DB}"
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        workspace._read_workspace_config()

        assert workspace.config["connections"]["db1"]["server"] == "prod.example.com"
        assert workspace.config["connections"]["db1"]["database"] == "production"

    def test_read_workspace_config_expands_env_vars_with_default(self, tmp_path):
        """Test that workspace config expands env vars with default values."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
connections:
  db1:
    server: "${MISSING_VAR:-localhost}"
    port: "${MISSING_PORT:-1433}"
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        workspace._read_workspace_config()

        assert workspace.config["connections"]["db1"]["server"] == "localhost"
        assert workspace.config["connections"]["db1"]["port"] == "1433"

    def test_read_workspace_config_raises_on_missing_env_var(self, tmp_path):
        """Test workspace config raises error on missing env var without default."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
connections:
  db1:
    server: "${MISSING_VAR}"
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        with pytest.raises(ConfigError, match="Environment variable 'MISSING_VAR'"):
            workspace._read_workspace_config()


class TestWorkspaceFindFlows:
    """Test Workspace._find_flows() - finding flows in flows/ directory."""

    def test_find_flows_loads_single_flow(self, tmp_path):
        """Test finding and loading a single flow."""
        # Create workspace structure
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        flows_dir = tmp_path / "flows"
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
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        workspace._read_workspace_config()
        flows = workspace._find_flows()

        assert len(flows) == 1
        assert "test_flow" in flows
        assert isinstance(flows["test_flow"], FlowConfig)

    def test_find_flows_loads_multiple_flows(self, tmp_path):
        """Test finding and loading multiple flows."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        # Create first flow
        flow1_dir = flows_dir / "flow1"
        flow1_dir.mkdir()
        flow1_content = (
            'name: "flow1"\nhome:\n  type: "parquet"\n  path: "data1"'
            '\nstore:\n  type: "parquet"\n  path: "out1"'
        )
        (flow1_dir / "flow.yml").write_text(flow1_content)

        # Create second flow
        flow2_dir = flows_dir / "flow2"
        flow2_dir.mkdir()
        flow2_content = (
            'name: "flow2"\nhome:\n  type: "parquet"\n  path: "data2"'
            '\nstore:\n  type: "parquet"\n  path: "out2"'
        )
        (flow2_dir / "flow.yml").write_text(flow2_content)

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        workspace._read_workspace_config()
        flows = workspace._find_flows()

        assert len(flows) == 2
        assert "flow1" in flows
        assert "flow2" in flows

    def test_find_flows_raises_error_when_directory_missing(self, tmp_path):
        """Test that _find_flows() raises error when flows directory doesn't exist."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        workspace._read_workspace_config()

        with pytest.raises(ConfigError, match="Flows directory not found"):
            workspace._find_flows()

    def test_find_flows_raises_error_when_no_flows(self, tmp_path):
        """Test that _find_flows() raises error when no flows found."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        workspace._read_workspace_config()

        with pytest.raises(ConfigError, match="No flows found in directory"):
            workspace._find_flows()

    def test_find_flows_ignores_non_directories(self, tmp_path):
        """Test that _find_flows() ignores non-directory files."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        # Create a file (not a directory)
        (flows_dir / "not_a_flow.txt").write_text("This is not a flow")

        # Create actual flow
        flow_dir = flows_dir / "test_flow"
        flow_dir.mkdir()
        flow_content = (
            'name: "test_flow"\nhome:\n  type: "parquet"\n  path: "data"'
            '\nstore:\n  type: "parquet"\n  path: "out"'
        )
        (flow_dir / "flow.yml").write_text(flow_content)

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        workspace._read_workspace_config()
        flows = workspace._find_flows()

        assert len(flows) == 1
        assert "test_flow" in flows

    def test_find_flows_ignores_directories_without_flow_yml(self, tmp_path):
        """Test that _find_flows() ignores directories without flow.yml."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        # Create directory without flow.yml
        (flows_dir / "empty_dir").mkdir()

        # Create actual flow
        flow_dir = flows_dir / "test_flow"
        flow_dir.mkdir()
        flow_content = (
            'name: "test_flow"\nhome:\n  type: "parquet"\n  path: "data"'
            '\nstore:\n  type: "parquet"\n  path: "out"'
        )
        (flow_dir / "flow.yml").write_text(flow_content)

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        workspace._read_workspace_config()
        flows = workspace._find_flows()

        assert len(flows) == 1
        assert "test_flow" in flows


class TestWorkspaceReadFlowConfig:
    """Test Workspace._read_flow_config() - reading individual flow.yml."""

    def test_read_flow_config_loads_basic_flow(self, tmp_path):
        """Test reading basic flow configuration."""
        flow_dir = tmp_path / "test_flow"
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

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        flow_config = workspace._read_flow_config(flow_dir)

        assert isinstance(flow_config, FlowConfig)
        assert flow_config.home["type"] == "parquet"
        assert flow_config.store["type"] == "parquet"

    def test_read_flow_config_loads_entities(self, tmp_path):
        """Test reading flow configuration with entities."""
        flow_dir = tmp_path / "test_flow"
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
  key_column: "id"
  batch_size: 10000
"""
        )

        # Create entities directory
        entities_dir = flow_dir / "entities"
        entities_dir.mkdir()

        # Create entity files
        (entities_dir / "users.yml").write_text(
            """
name: "users"
columns:
  - id
  - name
  - email
"""
        )

        (entities_dir / "orders.yml").write_text(
            """
name: "orders"
columns:
  - id
  - user_id
  - amount
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        flow_config = workspace._read_flow_config(flow_dir)

        assert flow_config.entities is not None
        assert len(flow_config.entities) == 2

        # Extract entity names (order may vary due to glob)
        entity_names = {entity["name"] for entity in flow_config.entities}
        assert "users" in entity_names
        assert "orders" in entity_names

        # Check that defaults are merged for all entities
        entity_dict = {entity["name"]: entity for entity in flow_config.entities}
        assert entity_dict["users"]["key_column"] == "id"
        assert entity_dict["users"]["batch_size"] == 10000
        assert entity_dict["orders"]["key_column"] == "id"

    def test_read_flow_config_expands_env_vars(self, tmp_path, monkeypatch):
        """Test that flow config expands environment variables."""
        monkeypatch.setenv("DATA_PATH", "/app/data")
        monkeypatch.setenv("OUTPUT_PATH", "/app/output")

        flow_dir = tmp_path / "test_flow"
        flow_dir.mkdir()

        flow_file = flow_dir / "flow.yml"
        flow_file.write_text(
            """
name: "test_flow"
home:
  type: "parquet"
  path: "${DATA_PATH}/source"
store:
  type: "parquet"
  path: "${OUTPUT_PATH}/destination"
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        flow_config = workspace._read_flow_config(flow_dir)

        assert flow_config.home["path"] == "/app/data/source"
        assert flow_config.store["path"] == "/app/output/destination"


class TestWorkspaceReadEntities:
    """Test Workspace._read_entities() - reading entity definitions."""

    def test_read_entities_loads_entity_files(self, tmp_path):
        """Test reading entity files from entities directory."""
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()

        defaults = {"key_column": "id", "batch_size": 10000}

        # Create entity files
        (entities_dir / "users.yml").write_text(
            """
name: "users"
columns:
  - id
  - name
"""
        )

        (entities_dir / "orders.yml").write_text(
            """
name: "orders"
columns:
  - id
  - user_id
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        entities = workspace._read_entities(entities_dir, defaults)

        assert len(entities) == 2

        # Extract entity names (order may vary due to glob)
        entity_names = {entity["name"] for entity in entities}
        assert "users" in entity_names
        assert "orders" in entity_names

        # Check that defaults are merged for all entities
        entity_dict = {entity["name"]: entity for entity in entities}
        assert entity_dict["users"]["key_column"] == "id"
        assert entity_dict["users"]["batch_size"] == 10000
        assert entity_dict["orders"]["key_column"] == "id"

    def test_read_entities_merges_defaults(self, tmp_path):
        """Test that entity defaults are properly merged."""
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()

        defaults = {"key_column": "id", "batch_size": 10000}

        (entities_dir / "users.yml").write_text(
            """
name: "users"
batch_size: 5000
columns:
  - id
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        entities = workspace._read_entities(entities_dir, defaults)

        assert len(entities) == 1
        # Entity-specific value should override default
        assert entities[0]["batch_size"] == 5000
        # Default should still be present
        assert entities[0]["key_column"] == "id"

    def test_read_entities_returns_empty_list_when_no_entities(self, tmp_path):
        """Test that _read_entities() returns empty list when no entities."""
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        entities = workspace._read_entities(entities_dir, {})

        assert entities == []

    def test_read_entities_expands_env_vars(self, tmp_path, monkeypatch):
        """Test that entity configs expand environment variables."""
        monkeypatch.setenv("TABLE_NAME", "users_prod")

        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()

        (entities_dir / "users.yml").write_text(
            """
name: "${TABLE_NAME}"
columns:
  - id
"""
        )

        workspace = Workspace(tmp_path / "hygge.yml", "test_project", "flows")
        entities = workspace._read_entities(entities_dir, {})

        assert len(entities) == 1
        assert entities[0]["name"] == "users_prod"


class TestWorkspacePrepare:
    """Test Workspace.prepare() - returning CoordinatorConfig with flows."""

    def test_prepare_returns_coordinator_config(self, tmp_path):
        """Test that prepare() returns CoordinatorConfig."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
connections:
  db1:
    type: "mssql"
    server: "localhost"
options:
  log_level: "DEBUG"
"""
        )

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        flow_dir = flows_dir / "test_flow"
        flow_dir.mkdir()
        (flow_dir / "flow.yml").write_text(
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

        workspace = Workspace.find(start_path=tmp_path)
        config = workspace.prepare()

        assert isinstance(config, CoordinatorConfig)
        assert len(config.flows) == 1
        assert "test_flow" in config.flows
        assert len(config.connections) == 1
        assert "db1" in config.connections

    def test_prepare_loads_multiple_flows(self, tmp_path):
        """Test that prepare() loads multiple flows."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        # Create multiple flows
        for flow_name in ["flow1", "flow2", "flow3"]:
            flow_dir = flows_dir / flow_name
            flow_dir.mkdir()
            (flow_dir / "flow.yml").write_text(
                f"""
name: "{flow_name}"
home:
  type: "parquet"
  path: "data/{flow_name}"
store:
  type: "parquet"
  path: "out/{flow_name}"
"""
            )

        workspace = Workspace.find(start_path=tmp_path)
        config = workspace.prepare()

        assert len(config.flows) == 3
        assert "flow1" in config.flows
        assert "flow2" in config.flows
        assert "flow3" in config.flows

    def test_prepare_includes_journal_config(self, tmp_path):
        """Test that prepare() includes journal configuration."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text(
            """
name: "test_project"
flows_dir: "flows"
journal:
  path: "/tmp/journal"
"""
        )

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        flow_dir = flows_dir / "test_flow"
        flow_dir.mkdir()
        (flow_dir / "flow.yml").write_text(
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

        workspace = Workspace.find(start_path=tmp_path)
        config = workspace.prepare()

        assert config.journal is not None
        assert config.journal.path == "/tmp/journal"


class TestWorkspaceEdgeCases:
    """Test edge cases and error scenarios."""

    def test_workspace_with_custom_flows_dir(self, tmp_path):
        """Test workspace with custom flows_dir."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"\nflows_dir: "my_flows"')

        my_flows_dir = tmp_path / "my_flows"
        my_flows_dir.mkdir()

        flow_dir = my_flows_dir / "test_flow"
        flow_dir.mkdir()
        (flow_dir / "flow.yml").write_text(
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

        workspace = Workspace.find(start_path=tmp_path)
        config = workspace.prepare()

        assert len(config.flows) == 1
        assert workspace.flows_dir == "my_flows"

    def test_workspace_handles_missing_optional_fields(self, tmp_path):
        """Test workspace handles missing optional fields gracefully."""
        hygge_file = tmp_path / "hygge.yml"
        hygge_file.write_text('name: "test_project"')

        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()

        flow_dir = flows_dir / "test_flow"
        flow_dir.mkdir()
        (flow_dir / "flow.yml").write_text(
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

        workspace = Workspace.find(start_path=tmp_path)
        config = workspace.prepare()

        # Should work even without connections or options
        assert len(config.flows) == 1
        assert config.connections == {}
        assert workspace.options == {}
