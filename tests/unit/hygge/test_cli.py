"""
Tests for hygge CLI functionality.

These tests verify that the CLI commands work correctly with the project-centric
approach and handle errors gracefully.
"""

import os
import sys
import tempfile
from pathlib import Path

import polars as pl
import pytest
import yaml

from hygge.cli import hygge

# Import implementations to register them
from hygge.homes.parquet.home import ParquetHome, ParquetHomeConfig  # noqa: F401
from hygge.stores.parquet.store import ParquetStore, ParquetStoreConfig  # noqa: F401


class TestHyggeInit:
    """Test hygge init command functionality."""

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_init_creates_project_structure(self, cli_runner):
        """Test that hygge init creates the correct project structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Change to temp directory
            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                # Run hygge init (now requires project name)
                result = cli_runner.invoke(hygge, ["init", "test_project"])

                # Check command succeeded
                assert result.exit_code == 0
                assert "hygge project initialized successfully" in result.output

                # Check files were created in project directory
                project_dir = temp_path / "test_project"
                assert (project_dir / "hygge.yml").exists()
                assert (project_dir / "flows").exists()
                assert (project_dir / "flows" / "example_flow").exists()
                assert (project_dir / "flows" / "example_flow" / "flow.yml").exists()
                assert (project_dir / "flows" / "example_flow" / "entities").exists()
                assert (
                    project_dir / "flows" / "example_flow" / "entities" / "users.yml"
                ).exists()

                # Check hygge.yml content
                hygge_config = yaml.safe_load((project_dir / "hygge.yml").read_text())
                assert hygge_config["name"] == "test_project"
                assert hygge_config["flows_dir"] == "flows"

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_init_with_custom_project_name(self, cli_runner):
        """Test hygge init with custom project name."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["init", "my_custom_project"])

                assert result.exit_code == 0

                # Check hygge.yml has custom name in project directory
                project_dir = temp_path / "my_custom_project"
                hygge_config = yaml.safe_load((project_dir / "hygge.yml").read_text())
                assert hygge_config["name"] == "my_custom_project"

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_init_with_custom_flows_dir(self, cli_runner):
        """Test hygge init with custom flows directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(
                    hygge, ["init", "test_project", "--flows-dir", "my_flows"]
                )

                assert result.exit_code == 0

                # Check custom flows directory was created in project directory
                project_dir = temp_path / "test_project"
                assert (project_dir / "my_flows").exists()

                # Check hygge.yml has custom flows_dir
                hygge_config = yaml.safe_load((project_dir / "hygge.yml").read_text())
                assert hygge_config["flows_dir"] == "my_flows"

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_init_fails_when_project_dir_exists(self, cli_runner):
        """Test that hygge init fails when project directory already exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create existing project directory
            project_dir = temp_path / "test_project"
            project_dir.mkdir()
            (project_dir / "hygge.yml").write_text("name: existing_project")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["init", "test_project"])

                assert result.exit_code == 1
                assert (
                    "Project directory 'test_project' already exists" in result.output
                )
                assert "Use --force to overwrite it" in result.output

            finally:
                os.chdir(original_cwd)

    def test_init_with_force_overwrites_existing(self, cli_runner):
        """Test that hygge init --force overwrites existing project directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create existing project directory
            project_dir = temp_path / "test_project"
            project_dir.mkdir()
            (project_dir / "hygge.yml").write_text("name: existing_project")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["init", "test_project", "--force"])

                assert result.exit_code == 0

                # Check hygge.yml was overwritten
                hygge_config = yaml.safe_load((project_dir / "hygge.yml").read_text())
                assert hygge_config["name"] == "test_project"

            finally:
                os.chdir(original_cwd)


class TestHyggeDebug:
    """Test hygge debug command functionality."""

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_debug_shows_project_info(self, cli_runner):
        """Test that hygge debug shows project information."""
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
"""
            )

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["debug"])

                assert result.exit_code == 0
                assert "Project configuration is valid" in result.output
                assert "Project: test_project" in result.output
                assert "Flows directory: flows" in result.output
                assert "Total flows: 1" in result.output
                assert "test_flow" in result.output

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_debug_fails_when_no_project(self, cli_runner):
        """Test that hygge debug fails when no hygge project found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            os.chdir(temp_dir)

            try:
                result = cli_runner.invoke(hygge, ["debug"])

                assert result.exit_code == 1
                assert (
                    "Configuration Error" in result.output
                    or "Configuration error" in result.output
                )

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_debug_shows_entities(self, cli_runner):
        """Test that hygge debug shows entity information."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create project with entities
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
"""
            )

            entities_dir = flow_dir / "entities"
            entities_dir.mkdir()

            entity_file = entities_dir / "users.yml"
            entity_file.write_text(
                """
name: "users"
columns:
  - id
  - name
"""
            )

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["debug"])

                assert result.exit_code == 0
                assert "entities ready to move" in result.output

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_debug_shows_warm_messages(self, cli_runner):
        """Test that hygge debug shows warm, friendly messages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create project structure
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

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
"""
            )

            # Create the paths so validation passes
            (temp_path / "data" / "source").mkdir(parents=True)
            (temp_path / "data").mkdir(exist_ok=True)

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["debug"])

                assert result.exit_code == 0
                # Check for warm welcome
                assert "🏡" in result.output or "hygge debug" in result.output
                # Check for friendly icons
                assert "✓" in result.output
                # Check for next steps
                assert "Next steps" in result.output or "hygge go" in result.output

            finally:
                os.chdir(original_cwd)

    def test_debug_validates_missing_paths(self, cli_runner):
        """Test that hygge debug warns about missing paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create project with flow pointing to non-existent paths
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

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
  path: "data/missing_source"
store:
  type: "parquet"
  path: "data/destination"
"""
            )

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["debug"])

                assert result.exit_code == 0
                # Should show path validation
                assert "Validating paths" in result.output
                # Should show warning about missing path
                assert "⚠️" in result.output or "doesn't exist" in result.output.lower()

            finally:
                os.chdir(original_cwd)


class TestHyggeGo:
    """Test hygge go command functionality."""

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_go_runs_flows(self, cli_runner):
        """Test that hygge go runs flows successfully."""
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
"""
            )

            # Create data directories for testing
            (temp_path / "data").mkdir()
            (temp_path / "data" / "source").mkdir()
            (temp_path / "data" / "destination").mkdir()

            # Create a simple parquet file so the flow has something to process
            test_data = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data.write_parquet(temp_path / "data" / "source" / "test.parquet")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["go"])

                # Should succeed even with no data
                assert result.exit_code == 0
                assert "Starting all flows" in result.output

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_go_fails_when_no_project(self, cli_runner):
        """Test that hygge go fails when no hygge project found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            os.chdir(temp_dir)

            try:
                result = cli_runner.invoke(hygge, ["go"])

                assert result.exit_code == 1
                assert "Configuration error" in result.output

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_go_with_flow_filter(self, cli_runner):
        """Test that hygge go --flow filters flows correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create project with multiple flows
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            # Create first flow
            flow1_dir = flows_dir / "flow1"
            flow1_dir.mkdir()
            (flow1_dir / "flow.yml").write_text(
                """
name: "flow1"
home:
  type: "parquet"
  path: "data/flow1"
store:
  type: "parquet"
  path: "out/flow1"
"""
            )

            # Create second flow
            flow2_dir = flows_dir / "flow2"
            flow2_dir.mkdir()
            (flow2_dir / "flow.yml").write_text(
                """
name: "flow2"
home:
  type: "parquet"
  path: "data/flow2"
store:
  type: "parquet"
  path: "out/flow2"
"""
            )

            # Create data directories and files
            (temp_path / "data" / "flow1").mkdir(parents=True)
            (temp_path / "out" / "flow1").mkdir(parents=True)
            test_data = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data.write_parquet(temp_path / "data" / "flow1" / "test.parquet")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["go", "--flow", "flow1"])

                # Should succeed and only run flow1
                assert result.exit_code == 0
                assert "Starting flows: flow1" in result.output

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_go_with_incremental_flag(self, cli_runner):
        """Test that hygge go --incremental sets run_type correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

            flows_dir = temp_path / "flows"
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

            # Create data directories and files
            (temp_path / "data" / "source").mkdir(parents=True)
            (temp_path / "data" / "destination").mkdir(parents=True)
            test_data = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data.write_parquet(temp_path / "data" / "source" / "test.parquet")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["go", "--incremental"])

                # Should succeed with incremental mode
                assert result.exit_code == 0
                assert "Starting all flows" in result.output

            finally:
                os.chdir(original_cwd)

    def test_go_with_full_drop_flag(self, cli_runner):
        """Test that hygge go --full-drop sets run_type correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

            flows_dir = temp_path / "flows"
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

            # Create data directories and files
            (temp_path / "data" / "source").mkdir(parents=True)
            (temp_path / "data" / "destination").mkdir(parents=True)
            test_data = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data.write_parquet(temp_path / "data" / "source" / "test.parquet")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["go", "--full-drop"])

                # Should succeed with full_drop mode
                assert result.exit_code == 0
                assert "Starting all flows" in result.output

            finally:
                os.chdir(original_cwd)

    def test_go_with_flow_var_override(self, cli_runner):
        """Test that hygge go --var overrides flow configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

            flows_dir = temp_path / "flows"
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

            # Create data directories and files
            (temp_path / "data" / "source").mkdir(parents=True)
            (temp_path / "data" / "destination").mkdir(parents=True)
            test_data = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data.write_parquet(temp_path / "data" / "source" / "test.parquet")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(
                    hygge,
                    [
                        "go",
                        "--var",
                        "flow.test_flow.run_type=incremental",
                    ],
                )

                # Should succeed with var override
                assert result.exit_code == 0

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_go_with_entity_filter(self, cli_runner):
        """Test that hygge go --entity filters entity flows correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

            flows_dir = temp_path / "flows"
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

            # Create entities directory
            entities_dir = flow_dir / "entities"
            entities_dir.mkdir()

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

            # Create data directories and files
            # Entity flows append entity_name to paths, so create users-specific paths
            (temp_path / "data" / "source" / "users").mkdir(parents=True)
            (temp_path / "data" / "destination" / "users").mkdir(parents=True)
            test_data = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data.write_parquet(
                temp_path / "data" / "source" / "users" / "test.parquet"
            )

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["go", "--entity", "test_flow.users"])

                # Should succeed and only run users entity flow
                assert result.exit_code == 0
                assert "Starting flows: test_flow_users" in result.output

            finally:
                os.chdir(original_cwd)

    def test_go_with_concurrency_override(self, cli_runner):
        """Test that hygge go --concurrency overrides default concurrency."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

            flows_dir = temp_path / "flows"
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

            # Create data directories and files
            (temp_path / "data" / "source").mkdir(parents=True)
            (temp_path / "data" / "destination").mkdir(parents=True)
            test_data = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data.write_parquet(temp_path / "data" / "source" / "test.parquet")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["go", "--concurrency", "4"])

                # Should succeed with custom concurrency
                assert result.exit_code == 0

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_go_with_multiple_flows_comma_separated(self, cli_runner):
        """Test that hygge go --flow accepts comma-separated flow names."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create project with multiple flows
            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

            flows_dir = temp_path / "flows"
            flows_dir.mkdir()

            # Create first flow
            flow1_dir = flows_dir / "flow1"
            flow1_dir.mkdir()
            (flow1_dir / "flow.yml").write_text(
                """
name: "flow1"
home:
  type: "parquet"
  path: "data/flow1"
store:
  type: "parquet"
  path: "out/flow1"
"""
            )

            # Create second flow
            flow2_dir = flows_dir / "flow2"
            flow2_dir.mkdir()
            (flow2_dir / "flow.yml").write_text(
                """
name: "flow2"
home:
  type: "parquet"
  path: "data/flow2"
store:
  type: "parquet"
  path: "out/flow2"
"""
            )

            # Create data directories and files for both flows
            (temp_path / "data" / "flow1").mkdir(parents=True)
            (temp_path / "data" / "flow2").mkdir(parents=True)
            (temp_path / "out" / "flow1").mkdir(parents=True)
            (temp_path / "out" / "flow2").mkdir(parents=True)
            test_data = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data.write_parquet(temp_path / "data" / "flow1" / "test.parquet")
            test_data.write_parquet(temp_path / "data" / "flow2" / "test.parquet")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                # Test comma-separated flows: --flow flow1,flow2
                result = cli_runner.invoke(hygge, ["go", "--flow", "flow1,flow2"])

                # Should succeed and run both flows
                assert result.exit_code == 0
                assert "Starting flows: flow1, flow2" in result.output

            finally:
                os.chdir(original_cwd)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Temp dir teardown fails on Windows when hygge.log is held open",
    )
    def test_go_with_multiple_entities_comma_separated(self, cli_runner):
        """Test that hygge go --entity accepts comma-separated entity names."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            hygge_file = temp_path / "hygge.yml"
            hygge_file.write_text('name: "test_project"\nflows_dir: "flows"')

            flows_dir = temp_path / "flows"
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

            # Create entities directory
            entities_dir = flow_dir / "entities"
            entities_dir.mkdir()

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

            # Create data directories and files for both entities
            (temp_path / "data" / "source" / "users").mkdir(parents=True)
            (temp_path / "data" / "source" / "orders").mkdir(parents=True)
            (temp_path / "data" / "destination" / "users").mkdir(parents=True)
            (temp_path / "data" / "destination" / "orders").mkdir(parents=True)

            test_data_users = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data_users.write_parquet(
                temp_path / "data" / "source" / "users" / "test.parquet"
            )

            test_data_orders = pl.DataFrame({"id": [1, 2], "user_id": [1, 2]})
            test_data_orders.write_parquet(
                temp_path / "data" / "source" / "orders" / "test.parquet"
            )

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                # Test comma-separated entities: --entity flow.entity1,flow.entity2
                result = cli_runner.invoke(
                    hygge, ["go", "--entity", "test_flow.users,test_flow.orders"]
                )

                # Should succeed and run both entity flows
                assert result.exit_code == 0
                assert "test_flow_users" in result.output
                assert "test_flow_orders" in result.output

            finally:
                os.chdir(original_cwd)


@pytest.fixture
def cli_runner():
    """Fixture providing Click CLI test runner."""
    from click.testing import CliRunner

    return CliRunner()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
