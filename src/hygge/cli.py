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
from hygge.core.workspace import Workspace
from hygge.messages import ErrorFormatter, get_logger
from hygge.utility.exceptions import ConfigError, HyggeError


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

    # Warm welcome message
    click.echo("✨ Welcome to hygge - comfortable data movement!")
    click.echo("")

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

    click.echo("\n🎉 hygge project initialized successfully!")
    click.echo("\n📋 Next steps:")
    click.echo(f"  1. cd {project_name}")
    click.echo(
        f"  2. Edit {flow_file.relative_to(project_dir)} to configure your "
        "data sources"
    )
    click.echo("  3. Update paths in flow.yml to point to your actual data locations")
    click.echo("  4. Run: hygge go")
    click.echo("\n💡 Tip: Use 'hygge debug' to test your configuration before running flows")

    logger.info(f"Initialized hygge project '{project_name}' in {project_dir}")


@hygge.command()
@click.option(
    "--flow",
    "-f",
    multiple=True,
    help=(
        "Run specific flow(s) by name. Can be specified multiple times or "
        "comma-separated. Supports base flow names (e.g., 'salesforce') or "
        "entity flow names (e.g., 'salesforce_Involvement'). "
        "Example: --flow salesforce --flow users_to_lake"
    ),
)
@click.option(
    "--entity",
    "-e",
    multiple=True,
    help=(
        "Run specific entity(ies) within a flow. Format: flow.entity. "
        "Example: --entity salesforce.Involvement --entity salesforce.Account"
    ),
)
@click.option(
    "--incremental",
    "-i",
    is_flag=True,
    help="Run flows in incremental mode (appends data instead of truncating)",
)
@click.option(
    "--full-drop",
    "-d",
    is_flag=True,
    help="Run flows in full_drop mode (truncates destination before writing)",
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
    "-v",
    is_flag=True,
    help="Enable verbose logging and show full stack traces for errors",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Preview what would be executed without actually running flows",
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
def go(
    flow: tuple,
    entity: tuple,
    incremental: bool,
    full_drop: bool,
    concurrency: Optional[int],
    verbose: bool,
    var: tuple,
    dry_run: bool,
):
    """Execute flows in the current hygge project."""
    logger = get_logger("hygge.cli.go")

    # Check for first-run (no hygge.yml exists)
    try:
        workspace = Workspace.find()
    except ConfigError:
        # First run - show welcome
        click.echo("✨ Welcome to hygge!")
        click.echo("")
        click.echo("It looks like this is your first time running hygge here.")
        click.echo("To get started, run: hygge init <project_name>")
        click.echo("")
        sys.exit(0)

    if verbose:
        # TODO: Set log level to DEBUG
        pass

    # Validate run_type flags
    if incremental and full_drop:
        click.echo("Error: Cannot specify both --incremental and --full-drop")
        sys.exit(1)

    try:
        # Parse flow filter from --flow and --entity options
        flow_filter = []

        # Parse --flow options (can be comma-separated)
        for flow_str in flow:
            # Split comma-separated flows
            flows = [f.strip() for f in flow_str.split(",") if f.strip()]
            flow_filter.extend(flows)

        # Parse --entity options (format: flow.entity)
        for entity_str in entity:
            # Split comma-separated entities
            entities = [e.strip() for e in entity_str.split(",") if e.strip()]
            for entity_spec in entities:
                if "." not in entity_spec:
                    click.echo(
                        f"Error: Invalid --entity format: {entity_spec}. "
                        f"Use flow.entity (e.g., salesforce.Involvement)"
                    )
                    sys.exit(1)
                flow_name, entity_name = entity_spec.split(".", 1)
                # Convert to entity flow name format
                entity_flow_name = f"{flow_name}_{entity_name}"
                flow_filter.append(entity_flow_name)

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

        # Use Workspace to load configuration (replaces temp coordinator hack)
        # workspace already found above for first-run check
        config = workspace.prepare()

        # Apply run_type override if specified
        if incremental:
            run_type_override = "incremental"
        elif full_drop:
            run_type_override = "full_drop"
        else:
            run_type_override = None

        # If run_type override specified, apply to all flows (filtered or all)
        if run_type_override:
            # Determine which base flows to override
            if flow_filter:
                # Extract base flow names from filter
                base_flow_names = set()
                for flow_name in flow_filter:
                    # Check if it's an entity flow (contains underscore)
                    if "_" in flow_name:
                        # Try to extract base flow name
                        parts = flow_name.rsplit("_", 1)
                        if len(parts) == 2:
                            potential_base = parts[0]
                            # Check if this base flow exists in config entities
                            if any(
                                e.base_flow_name == potential_base
                                for e in config.entities
                            ):
                                base_flow_names.add(potential_base)
                                continue
                    # Not an entity flow or base flow not found, use as-is
                    if any(
                        e.flow_name == flow_name or e.base_flow_name == flow_name
                        for e in config.entities
                    ):
                        # Find the base flow name
                        for entity in config.entities:
                            if (
                                entity.flow_name == flow_name
                                or entity.base_flow_name == flow_name
                            ):
                                base_flow_names.add(entity.base_flow_name)
                                break
            else:
                # Apply to all flows - get unique base flow names from entities
                base_flow_names = {e.base_flow_name for e in config.entities}

            # Apply run_type override to all base flows
            for base_flow_name in base_flow_names:
                if base_flow_name not in flow_overrides:
                    flow_overrides[base_flow_name] = {}
                flow_overrides[base_flow_name]["run_type"] = run_type_override

        # Create coordinator with prepared config and final overrides
        # Pass config directly to avoid Workspace.find() being called again
        coordinator = Coordinator(
            config=config,
            flow_overrides=flow_overrides if flow_overrides else None,
            flow_filter=flow_filter if flow_filter else None,
        )

        # Apply CLI concurrency override (will override config options)
        if concurrency is not None:
            coordinator.options["concurrency"] = concurrency

        # Handle dry-run mode
        if dry_run:
            asyncio.run(coordinator.dry_run())
            click.echo("\n✨ Dry-run completed. No data was moved.")
            return

        # Run flows
        if flow_filter:
            flow_list = ", ".join(flow_filter)
            click.echo(f"Starting flows: {flow_list}")
        else:
            click.echo("Starting all flows...")

        asyncio.run(coordinator.run())
        click.echo("All flows completed successfully!")

    except ConfigError as e:
        friendly_msg, suggestion = ErrorFormatter.format_error(e, verbose=verbose)
        click.echo(f"Configuration error: {friendly_msg}", err=True)
        if suggestion:
            click.echo(f"\n💡 {suggestion}", err=True)
        if verbose:
            click.echo("\n" + ErrorFormatter.format_with_stack_trace(e), err=True)
        sys.exit(1)
    except HyggeError as e:
        friendly_msg, suggestion = ErrorFormatter.format_error(e, verbose=verbose)
        click.echo(f"Error: {friendly_msg}", err=True)
        if suggestion:
            click.echo(f"\n💡 {suggestion}", err=True)
        if verbose:
            click.echo("\n" + ErrorFormatter.format_with_stack_trace(e), err=True)
        logger.error(f"Error running flows: {e}")
        sys.exit(1)
    except Exception as e:
        friendly_msg, suggestion = ErrorFormatter.format_error(e, verbose=verbose)
        click.echo(f"Error: {friendly_msg}", err=True)
        if suggestion:
            click.echo(f"\n💡 {suggestion}", err=True)
        if verbose:
            click.echo("\n" + ErrorFormatter.format_with_stack_trace(e), err=True)
        logger.error(f"Error running flows: {e}")
        sys.exit(1)


@hygge.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show detailed connection test information",
)
def debug(verbose: bool):
    """Debug hygge project configuration and test all connections."""
    logger = get_logger("hygge.cli.debug")

    try:
        # Use Workspace to load configuration
        workspace = Workspace.find()
        config = workspace.prepare()

        click.echo("🔍 Validating hygge project configuration...")
        click.echo("")

        click.echo("✅ Project configuration is valid")
        click.echo(f"   Project: {workspace.name}")
        click.echo(f"   Flows directory: {workspace.flows_dir}")
        click.echo(f"   Number of entities: {len(config.entities)}")
        click.echo("")

        # Group entities by base_flow_name
        flows_dict = {}
        for entity in config.entities:
            base_flow = entity.base_flow_name
            if base_flow not in flows_dict:
                flows_dict[base_flow] = []
            flows_dict[base_flow].append(entity)

        # List flows and their entities
        click.echo("📊 Configured flows:")
        for base_flow_name, entities in flows_dict.items():
            click.echo(f"   • {base_flow_name}")
            if len(entities) > 1 or (len(entities) == 1 and entities[0].entity_name):
                click.echo(f"      Entities: {len(entities)}")
                for entity in entities:
                    if entity.entity_name:
                        click.echo(
                            f"         - {entity.entity_name} ({entity.flow_name})"
                        )
                    else:
                        click.echo(f"         - {entity.flow_name} (implicit)")
        click.echo("")

        # Test all configured connections
        connections = config.connections
        if connections:
            click.echo(f"🔌 Testing {len(connections)} database connection(s)...")
            click.echo("")

            all_passed = True
            for conn_name, conn_config in connections.items():
                click.echo(f"   Testing: {conn_name}")
                click.echo(f"      Type: {conn_config.get('type', 'unknown')}")
                if verbose:
                    click.echo(f"      Server: {conn_config.get('server', 'unknown')}")
                    click.echo(f"      Database: {conn_config.get('database', 'unknown')}")

                try:
                    # Test the connection based on type
                    conn_type = conn_config.get("type", "").lower()

                    if conn_type == "mssql":
                        asyncio.run(_test_mssql_connection(conn_name, conn_config, verbose))
                        click.echo(f"      ✅ Connection successful")
                    else:
                        click.echo(
                            f"      ⚠️  Connection type '{conn_type}' not supported for testing"
                        )

                except Exception as e:
                    all_passed = False
                    friendly_msg, suggestion = ErrorFormatter.format_error(e, verbose=verbose)
                    click.echo(f"      ❌ Connection failed: {friendly_msg}", err=True)
                    if suggestion:
                        click.echo(f"      💡 {suggestion}", err=True)
                    if verbose:
                        click.echo("\n" + ErrorFormatter.format_with_stack_trace(e), err=True)
                click.echo("")

            if all_passed:
                click.echo("✅ All connections tested successfully!")
            else:
                click.echo("⚠️  Some connections failed. Check the errors above.")
        else:
            click.echo("ℹ️  No database connections configured")
            click.echo("")

        # Test file paths if configured
        click.echo("📁 Validating file paths...")
        path_issues = []
        for entity in config.entities:
            # Check home path
            if hasattr(entity.flow_config, "home") and hasattr(entity.flow_config.home, "path"):
                home_path = entity.flow_config.home.path
                if home_path:
                    from pathlib import Path
                    path_obj = Path(home_path)
                    if not path_obj.exists() and not path_obj.is_absolute():
                        # Try relative to workspace
                        workspace_path = workspace.hygge_yml.parent / path_obj
                        if not workspace_path.exists():
                            path_issues.append(f"Home path not found: {home_path} (flow: {entity.flow_name})")

            # Check store path
            if hasattr(entity.flow_config, "store") and hasattr(entity.flow_config.store, "path"):
                store_path = entity.flow_config.store.path
                if store_path:
                    from pathlib import Path
                    path_obj = Path(store_path)
                    # Store paths don't need to exist (will be created)
                    pass

        if path_issues:
            click.echo("   ⚠️  Found path issues:")
            for issue in path_issues:
                click.echo(f"      • {issue}")
        else:
            click.echo("   ✅ All file paths validated")
        click.echo("")

        click.echo("✨ Configuration validation complete!")

        logger.info("Project configuration debug completed")

    except ConfigError as e:
        friendly_msg, suggestion = ErrorFormatter.format_error(e, verbose=False)
        click.echo(f"Configuration error: {friendly_msg}", err=True)
        if suggestion:
            click.echo(f"\n💡 {suggestion}", err=True)
        sys.exit(1)
    except HyggeError as e:
        friendly_msg, suggestion = ErrorFormatter.format_error(e, verbose=False)
        click.echo(f"Error: {friendly_msg}", err=True)
        if suggestion:
            click.echo(f"\n💡 {suggestion}", err=True)
        logger.error(f"Error debugging configuration: {e}")
        sys.exit(1)
    except Exception as e:
        friendly_msg, suggestion = ErrorFormatter.format_error(e, verbose=False)
        click.echo(f"Error: {friendly_msg}", err=True)
        if suggestion:
            click.echo(f"\n💡 {suggestion}", err=True)
        logger.error(f"Error debugging configuration: {e}")
        sys.exit(1)


async def _test_mssql_connection(conn_name: str, conn_config: dict, verbose: bool = False):
    """Test MSSQL connection with detailed feedback."""
    if verbose:
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
        if verbose:
            click.echo("      Testing query...")
        connection = await connection_factory.get_connection()

        # Run a simple test query
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test_value")
        result = cursor.fetchone()
        cursor.close()
        connection.close()

        if verbose:
            click.echo(f"      Test query returned: {result[0]}")

    except Exception as e:
        raise Exception(f"MSSQL connection test failed: {str(e)}")


if __name__ == "__main__":
    hygge()
