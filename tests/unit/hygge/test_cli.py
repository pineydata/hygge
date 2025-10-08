"""
Tests for hygge CLI functionality.

These tests verify that the CLI commands work correctly with the project-centric
approach and handle errors gracefully.
"""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from hygge.cli import hygge

# Import implementations to register them
from hygge.homes.parquet.home import ParquetHome, ParquetHomeConfig  # noqa: F401
from hygge.stores.parquet.store import ParquetStore, ParquetStoreConfig  # noqa: F401


class TestHyggeInit:
    """Test hygge init command functionality."""

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
                assert "Number of flows: 1" in result.output
                assert "test_flow" in result.output

            finally:
                os.chdir(original_cwd)

    def test_debug_fails_when_no_project(self, cli_runner):
        """Test that hygge debug fails when no hygge project found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            os.chdir(temp_dir)

            try:
                result = cli_runner.invoke(hygge, ["debug"])

                assert result.exit_code == 1
                assert "Configuration error" in result.output

            finally:
                os.chdir(original_cwd)

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
                assert "Entities: 1" in result.output

            finally:
                os.chdir(original_cwd)


class TestHyggeStart:
    """Test hygge start command functionality."""

    def test_start_runs_flows(self, cli_runner):
        """Test that hygge start runs flows successfully."""
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
            import polars as pl

            test_data = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            test_data.write_parquet(temp_path / "data" / "source" / "test.parquet")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hygge, ["start"])

                # Should succeed even with no data
                assert result.exit_code == 0
                assert "Starting all flows" in result.output

            finally:
                os.chdir(original_cwd)

    def test_start_fails_when_no_project(self, cli_runner):
        """Test that hygge start fails when no hygge project found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            os.chdir(temp_dir)

            try:
                result = cli_runner.invoke(hygge, ["start"])

                assert result.exit_code == 1
                assert "Configuration error" in result.output

            finally:
                os.chdir(original_cwd)


@pytest.fixture
def cli_runner():
    """Fixture providing Click CLI test runner."""
    from click.testing import CliRunner

    return CliRunner()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
