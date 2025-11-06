#!/usr/bin/env python3
"""
hygge CLI - Comfortable data movement framework command-line interface.

This module provides the `hygge` command-line interface for hygge projects.
"""

import asyncio
import sys
from pathlib import Path
from typing import Any, Optional

import click

from hygge import Coordinator
from hygge.utility.exceptions import ConfigError
from hygge.utility.logger import get_logger


def _parse_var_value(value: str) -> Any:
    """Parse CLI variable value to appropriate type."""
    if value.lower() in ("true", "1", "yes"):
        return True
    elif value.lower() in ("false", "0", "no"):
        return False
    elif value.isdigit():
        return int(value)
    elif value.replace(".", "", 1).isdigit():
        return float(value)
    else:
        return value


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
            f"Project directory '{project_name}' already exists in {current_dir}"
        )
        click.echo("Use --force to overwrite it")
        sys.exit(1)

    # Create project directory
    project_dir.mkdir(exist_ok=True)
    click.echo(f"Created project directory: {project_dir}")

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
    click.echo(f"Created hygge.yml for project '{project_name}'")

    # Create flows directory
    flows_path = project_dir / flows_dir
    flows_path.mkdir(exist_ok=True)
    click.echo("Created flows directory: {}".format(flows_path))

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
    click.echo("Created example flow: {}".format(flow_file))

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
    click.echo(f"Created example entity: {entity_file}")

    click.echo("\nhygge project initialized successfully!")
    click.echo("\nNext steps:")
    click.echo(f"  1. cd {project_name}")
    click.echo(
        f"  2. Edit {flow_file.relative_to(project_dir)} to configure your "
        "data sources"
    )
    click.echo("  3. Update paths in flow.yml to point to your actual data locations")
    click.echo("  4. Run: hygge go")

    logger.info(f"Initialized hygge project '{project_name}' in {project_dir}")


@hygge.command()
@click.option(
    "--flow",
    "-f",
    help="Run specific flow by name",
)
@click.option(
    "--concurrency",
    "-c",
    type=int,
    help=(
        "Maximum number of flows to run concurrently "
        "(default: matches connection pool size, or 8). "
        "Uses async tasks with a semaphore to limit concurrency."
    ),
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
@click.option(
    "--var",
    multiple=True,
    help=(
        "Override flow configuration values. "
        "Format: flow.<flow_name>.field=value. "
        "Example: --var flow.mssql_to_mirrored_db.full_drop=true"
    ),
)
def go(flow: Optional[str], concurrency: Optional[int], verbose: bool, var: tuple):
    """Execute all flows in the current hygge project."""
    logger = get_logger("hygge.cli.go")

    if verbose:
        # TODO: Set log level to DEBUG
        pass

    try:
        # Parse CLI variable overrides
        # Supports: flow.<flow_name>.field=value (flow-level overrides)
        flow_overrides = {}  # Flow-specific overrides

        for var_str in var:
            # Format: flow.<flow_name>.full_drop=true
            if "=" not in var_str:
                example = "flow.mssql_to_mirrored_db.full_drop=true"
                click.echo(
                    f"Error: Invalid --var format: {var_str}. "
                    f"Use flow.<flow_name>.field=value "
                    f"(e.g., {example})"
                )
                sys.exit(1)

            key, value = var_str.split("=", 1)
            key_parts = key.split(".")

            if len(key_parts) < 3 or key_parts[0] != "flow":
                click.echo(
                    "Error: --var must start with 'flow.<flow_name>.' "
                    "(e.g., flow.mssql_to_mirrored_db.full_drop=true)"
                )
                sys.exit(1)

            # Format: flow.<flow_name>.field
            flow_name = key_parts[1]
            field_parts = key_parts[2:]

            if flow_name not in flow_overrides:
                flow_overrides[flow_name] = {}

            # Build nested dict for the field path
            current = flow_overrides[flow_name]
            for part in field_parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]

            final_key = field_parts[-1]
            current[final_key] = _parse_var_value(value)

        # Create coordinator (will discover project automatically)
        coordinator = Coordinator(
            flow_overrides=flow_overrides if flow_overrides else None
        )

        # Apply CLI concurrency override if provided
        if concurrency is not None:
            coordinator.options["concurrency"] = concurrency

        if flow:
            click.echo(f"Starting flow: {flow}")
            # TODO: Implement single flow execution
            click.echo("Single flow execution not yet implemented")
        else:
            click.echo("Starting all flows...")
            # Run all flows
            asyncio.run(coordinator.run())
            click.echo("All flows completed successfully!")

    except ConfigError as e:
        click.echo(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}")
        logger.error(f"Error running flows: {e}")
        sys.exit(1)


@hygge.command()
def debug():
    """Debug hygge project configuration and test all connections."""
    logger = get_logger("hygge.cli.debug")

    try:
        # Create coordinator to validate configuration
        coordinator = Coordinator()
        coordinator._load_config()

        click.echo("Project configuration is valid")
        project_name = coordinator.project_config.get("name", "unnamed")
        click.echo("Project: {}".format(project_name))
        flows_dir = coordinator.project_config.get("flows_dir", "flows")
        click.echo("Flows directory: {}".format(flows_dir))
        click.echo("Number of flows: {}".format(len(coordinator.config.flows)))

        # List flows
        for flow_name in coordinator.config.flows:
            flow_config = coordinator.config.flows[flow_name]
            click.echo(f"   {flow_name}")
            if flow_config.entities:
                click.echo(f"      Entities: {len(flow_config.entities)}")

        # Test all configured connections
        connections = coordinator.project_config.get("connections", {})
        if connections:
            click.echo(f"\nTesting {len(connections)} database connections...")

            for conn_name, conn_config in connections.items():
                click.echo(f"\n   Testing connection: {conn_name}")
                click.echo(f"      Type: {conn_config.get('type', 'unknown')}")
                click.echo(f"      Server: {conn_config.get('server', 'unknown')}")
                click.echo(f"      Database: {conn_config.get('database', 'unknown')}")

                try:
                    # Test the connection based on type
                    conn_type = conn_config.get("type", "").lower()

                    if conn_type == "mssql":
                        asyncio.run(_test_mssql_connection(conn_name, conn_config))
                    else:
                        click.echo(
                            f"      WARNING: Connection type "
                            f"'{conn_type}' not supported"
                        )

                except Exception as e:
                    click.echo(f"      Connection failed: {str(e)}")
                    # Show more detailed error information
                    import traceback

                    click.echo(f"      Error details: {traceback.format_exc()}")
        else:
            click.echo("\nNo database connections configured")

        logger.info("Project configuration debug completed")

    except ConfigError as e:
        click.echo(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}")
        logger.error(f"Error debugging configuration: {e}")
        sys.exit(1)


async def _test_mssql_connection(conn_name: str, conn_config: dict):
    """Test MSSQL connection with detailed feedback."""
    click.echo("      Connecting...")

    try:
        # Import here to avoid circular imports
        from hygge.connections import MssqlConnection

        # Create connection factory
        connection_factory = MssqlConnection(
            server=conn_config.get("server"),
            database=conn_config.get("database"),
            options={
                "driver": conn_config.get("driver", "ODBC Driver 18 for SQL Server"),
                "encrypt": conn_config.get("encrypt", "yes"),
                "trust_cert": conn_config.get("trust_cert", "no"),
                "timeout": conn_config.get("timeout", 30),
            },
        )

        # Test connection
        click.echo("      Testing query...")
        connection = await connection_factory.get_connection()

        # Run a simple test query
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test_value")
        result = cursor.fetchone()
        cursor.close()
        connection.close()

        click.echo(f"      Connection successful! Test query returned: {result[0]}")

    except Exception as e:
        raise Exception(f"MSSQL connection test failed: {str(e)}")


if __name__ == "__main__":
    hygge()
