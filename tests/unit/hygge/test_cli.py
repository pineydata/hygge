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

from hygge.cli import hej

# Import implementations to register them
from hygge.homes.parquet.home import ParquetHome, ParquetHomeConfig  # noqa: F401
from hygge.stores.parquet.store import ParquetStore, ParquetStoreConfig  # noqa: F401


class TestHejInit:
    """Test hej init command functionality."""

    def test_init_creates_project_structure(self, cli_runner):
        """Test that hej init creates the correct project structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Change to temp directory
            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                # Run hej init
                result = cli_runner.invoke(hej, ["init"])

                # Check command succeeded
                assert result.exit_code == 0
                assert "hygge project initialized successfully" in result.output

                # Check files were created
                assert (temp_path / "hygge.yml").exists()
                assert (temp_path / "flows").exists()
                assert (temp_path / "flows" / "example_flow").exists()
                assert (temp_path / "flows" / "example_flow" / "flow.yml").exists()
                assert (temp_path / "flows" / "example_flow" / "entities").exists()
                assert (
                    temp_path / "flows" / "example_flow" / "entities" / "users.yml"
                ).exists()

                # Check hygge.yml content
                hygge_config = yaml.safe_load((temp_path / "hygge.yml").read_text())
                assert hygge_config["name"] == temp_path.name
                assert hygge_config["flows_dir"] == "flows"

            finally:
                os.chdir(original_cwd)

    def test_init_with_custom_project_name(self, cli_runner):
        """Test hej init with custom project name."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(
                    hej, ["init", "--project-name", "my_custom_project"]
                )

                assert result.exit_code == 0

                # Check hygge.yml has custom name
                hygge_config = yaml.safe_load((temp_path / "hygge.yml").read_text())
                assert hygge_config["name"] == "my_custom_project"

            finally:
                os.chdir(original_cwd)

    def test_init_with_custom_flows_dir(self, cli_runner):
        """Test hej init with custom flows directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hej, ["init", "--flows-dir", "my_flows"])

                assert result.exit_code == 0

                # Check custom flows directory was created
                assert (temp_path / "my_flows").exists()

                # Check hygge.yml has custom flows_dir
                hygge_config = yaml.safe_load((temp_path / "hygge.yml").read_text())
                assert hygge_config["flows_dir"] == "my_flows"

            finally:
                os.chdir(original_cwd)

    def test_init_fails_when_hygge_yml_exists(self, cli_runner):
        """Test that hej init fails when hygge.yml already exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create existing hygge.yml
            (temp_path / "hygge.yml").write_text("name: existing_project")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hej, ["init"])

                assert result.exit_code == 1
                assert "hygge.yml already exists" in result.output
                assert "Use --force to overwrite it" in result.output

            finally:
                os.chdir(original_cwd)

    def test_init_with_force_overwrites_existing(self, cli_runner):
        """Test that hej init --force overwrites existing hygge.yml."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create existing hygge.yml
            (temp_path / "hygge.yml").write_text("name: existing_project")

            original_cwd = Path.cwd()
            os.chdir(temp_path)

            try:
                result = cli_runner.invoke(hej, ["init", "--force"])

                assert result.exit_code == 0

                # Check hygge.yml was overwritten
                hygge_config = yaml.safe_load((temp_path / "hygge.yml").read_text())
                assert (
                    hygge_config["name"] == temp_path.name
                )  # Should be current dir name

            finally:
                os.chdir(original_cwd)


class TestHejDebug:
    """Test hej debug command functionality."""

    def test_debug_shows_project_info(self, cli_runner):
        """Test that hej debug shows project information."""
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
                result = cli_runner.invoke(hej, ["debug"])

                assert result.exit_code == 0
                assert "Project configuration is valid" in result.output
                assert "Project: test_project" in result.output
                assert "Flows directory: flows" in result.output
                assert "Number of flows: 1" in result.output
                assert "test_flow" in result.output

            finally:
                os.chdir(original_cwd)

    def test_debug_fails_when_no_project(self, cli_runner):
        """Test that hej debug fails when no hygge project found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            os.chdir(temp_dir)

            try:
                result = cli_runner.invoke(hej, ["debug"])

                assert result.exit_code == 1
                assert "Configuration error" in result.output

            finally:
                os.chdir(original_cwd)

    def test_debug_shows_entities(self, cli_runner):
        """Test that hej debug shows entity information."""
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
                result = cli_runner.invoke(hej, ["debug"])

                assert result.exit_code == 0
                assert "Entities: 1" in result.output

            finally:
                os.chdir(original_cwd)


class TestHejStart:
    """Test hej start command functionality."""

    def test_start_runs_flows(self, cli_runner):
        """Test that hej start runs flows successfully."""
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
                result = cli_runner.invoke(hej, ["start"])

                # Should succeed even with no data
                assert result.exit_code == 0
                assert "Starting all flows" in result.output

            finally:
                os.chdir(original_cwd)

    def test_start_fails_when_no_project(self, cli_runner):
        """Test that hej start fails when no hygge project found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            os.chdir(temp_dir)

            try:
                result = cli_runner.invoke(hej, ["start"])

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
