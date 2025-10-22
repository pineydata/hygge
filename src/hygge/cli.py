#!/usr/bin/env python3
"""
hygge CLI - Comfortable data movement framework command-line interface.

This module provides the `hygge` command-line interface for hygge projects.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import click

from hygge import Coordinator
from hygge.utility.exceptions import ConfigError
from hygge.utility.logger import get_logger


@click.group()
@click.version_option()
def hygge():
    """
    hygge - comfortable data movement framework

    A cozy, comfortable data movement framework that makes data feel at home.
    """
    pass


@hygge.command()
@click.argument("project_name")
@click.option(
    "--flows-dir",
    "-d",
    default="flows",
    help="Name of the flows directory (default: flows)",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite existing project directory if it exists",
)
def init(project_name: str, flows_dir: str, force: bool):
    """Initialize a new hygge project in a new directory.

    PROJECT_NAME: Name of the project and directory to create
    """
    logger = get_logger("hygge.cli.init")

    current_dir = Path.cwd()
    project_dir = current_dir / project_name

    # Check if project directory already exists
    if project_dir.exists() and not force:
        click.echo(
            f"❌ Project directory '{project_name}' already exists in " f"{current_dir}"
        )
        click.echo("Use --force to overwrite it")
        sys.exit(1)

    # Create project directory
    project_dir.mkdir(exist_ok=True)
    click.echo(f"✅ Created project directory: {project_dir}")

    # Create hygge.yml
    hygge_content = f"""name: "{project_name}"
flows_dir: "{flows_dir}"

# Project-level options
options:
  log_level: INFO
  continue_on_error: false
"""

    hygge_file = project_dir / "hygge.yml"
    hygge_file.write_text(hygge_content)
    click.echo(f"✅ Created hygge.yml for project '{project_name}'")

    # Create flows directory
    flows_path = project_dir / flows_dir
    flows_path.mkdir(exist_ok=True)
    click.echo("✅ Created flows directory: {}".format(flows_path))

    # Create example flow
    example_flow_dir = flows_path / "example_flow"
    example_flow_dir.mkdir(exist_ok=True)

    # Create example flow.yml
    flow_content = """name: "example_flow"
home:
  type: "parquet"
  path: "data/source"
store:
  type: "parquet"
  path: "data/destination"

# Flow-level defaults
defaults:
  key_column: "id"
  batch_size: 10000
"""

    flow_file = example_flow_dir / "flow.yml"
    flow_file.write_text(flow_content)
    click.echo("✅ Created example flow: {}".format(flow_file))

    # Create example entities directory
    entities_dir = example_flow_dir / "entities"
    entities_dir.mkdir(exist_ok=True)

    # Create example entity
    entity_content = """name: "users"
columns:
  - id
  - name
  - email
  - created_at

# Optional source-specific configuration
source_config:
  where: "created_at > '2024-01-01'"
"""

    entity_file = entities_dir / "users.yml"
    entity_file.write_text(entity_content)
    click.echo(f"✅ Created example entity: {entity_file}")

    click.echo("\n🎉 hygge project initialized successfully!")
    click.echo("\nNext steps:")
    click.echo(f"  1. cd {project_name}")
    click.echo(
        f"  2. Edit {flow_file.relative_to(project_dir)} to configure your "
        "data sources"
    )
    click.echo("  3. Update paths in flow.yml to point to your actual data locations")
    click.echo("  4. Run: hygge start")

    logger.info(f"Initialized hygge project '{project_name}' in {project_dir}")


@hygge.command()
@click.option(
    "--flow",
    "-f",
    help="Run specific flow by name",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
def start(flow: Optional[str], verbose: bool):
    """Start all flows in the current hygge project."""
    logger = get_logger("hygge.cli.start")

    if verbose:
        # TODO: Set log level to DEBUG
        pass

    try:
        # Create coordinator (will discover project automatically)
        coordinator = Coordinator()

        if flow:
            click.echo(f"🚀 Starting flow: {flow}")
            # TODO: Implement single flow execution
            click.echo("Single flow execution not yet implemented")
        else:
            click.echo("🚀 Starting all flows...")
            # Run all flows
            asyncio.run(coordinator.run())
            click.echo("✅ All flows completed successfully!")

    except ConfigError as e:
        click.echo(f"❌ Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Error: {e}")
        logger.error(f"Error running flows: {e}")
        sys.exit(1)


@hygge.command()
def debug():
    """Debug hygge project configuration and show detailed information."""
    logger = get_logger("hygge.cli.debug")

    try:
        # Create coordinator to validate configuration
        coordinator = Coordinator()
        coordinator._load_config()

        click.echo("✅ Project configuration is valid")
        project_name = coordinator.project_config.get("name", "unnamed")
        click.echo("📁 Project: {}".format(project_name))
        flows_dir = coordinator.project_config.get("flows_dir", "flows")
        click.echo("📂 Flows directory: {}".format(flows_dir))
        click.echo("🔄 Number of flows: {}".format(len(coordinator.config.flows)))

        # List flows
        for flow_name in coordinator.config.flows:
            flow_config = coordinator.config.flows[flow_name]
            click.echo(f"   📄 {flow_name}")
            if flow_config.entities:
                click.echo(f"      Entities: {len(flow_config.entities)}")

        logger.info("Project configuration debug completed")

    except ConfigError as e:
        click.echo(f"❌ Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Error: {e}")
        logger.error(f"Error debugging configuration: {e}")
        sys.exit(1)


if __name__ == "__main__":
    hygge()
