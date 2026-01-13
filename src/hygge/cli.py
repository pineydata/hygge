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
from hygge.messages import get_logger
from hygge.utility.exceptions import ConfigError


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
        f"  2. Edit {flow_file.relative_to(project_dir)} to configure your data sources"
    )
    click.echo("  3. Update paths in flow.yml to point to your actual data locations")
    click.echo("  4. Run: hygge go")

    logger.info(f"Initialized hygge project '{project_name}' in {project_dir}")


@hygge.command()
@click.option(
    "--flow",
    "-f",
    "flow_names",
    help=(
        "Run specific flow(s) by name (comma-separated for multiple). "
        "Supports base flow names (e.g., 'salesforce') or "
        "entity flow names (e.g., 'salesforce_Involvement'). "
        "Examples: --flow salesforce OR --flow flow1,flow2,flow3"
    ),
)
@click.option(
    "--entity",
    "-e",
    "entity_names",
    help=(
        "Run specific entity(ies) within a flow (comma-separated for multiple). "
        "Format: flow.entity. "
        "Examples: --entity salesforce.Account OR --entity flow1.users,flow1.orders"
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
    "--dry-run",
    is_flag=True,
    help="Preview what would happen without moving data",
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
def go(
    flow_names: Optional[str],
    entity_names: Optional[str],
    incremental: bool,
    full_drop: bool,
    concurrency: Optional[int],
    dry_run: bool,
    verbose: bool,
    var: tuple,
):
    """Execute flows in the current hygge project."""
    logger = get_logger("hygge.cli.go")

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

        # Parse --flow option (comma-separated string)
        if flow_names:
            flows = [f.strip() for f in flow_names.split(",") if f.strip()]
            flow_filter.extend(flows)

        # Parse --entity option (comma-separated, format: flow.entity)
        if entity_names:
            entities = [e.strip() for e in entity_names.split(",") if e.strip()]
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
        workspace = Workspace.find()
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

        # Run flows or preview
        if dry_run:
            # Preview mode - show what would happen
            preview_results = asyncio.run(coordinator.preview(verbose=verbose))
            _print_preview(preview_results, verbose=verbose, flow_filter=flow_filter)
        else:
            # Normal execution
            if flow_filter:
                flow_list = ", ".join(flow_filter)
                click.echo(f"Starting flows: {flow_list}")
            else:
                click.echo("Starting all flows...")

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
        # Warm welcome
        click.echo("🏡 hygge debug - Let's make sure everything feels right\n")

        # Use Workspace to load configuration
        workspace = Workspace.find()
        config = workspace.prepare()

        click.echo("✓ Project configuration is valid")
        click.echo(f"  Project: {workspace.name}")
        click.echo(f"  Flows directory: {workspace.flows_dir}")
        click.echo(f"  Total flows: {len(config.entities)}\n")

        # Group entities by base_flow_name
        flows_dict = {}
        for entity in config.entities:
            base_flow = entity.base_flow_name
            if base_flow not in flows_dict:
                flows_dict[base_flow] = []
            flows_dict[base_flow].append(entity)

        # List flows and their entities with warm messaging
        click.echo("📋 Discovered Flows:")
        for base_flow_name, entities in flows_dict.items():
            click.echo(f"  • {base_flow_name}")
            if len(entities) > 1 or (len(entities) == 1 and entities[0].entity_name):
                click.echo(f"    {len(entities)} entities ready to move")
                for entity in entities:
                    if entity.entity_name:
                        click.echo(f"      - {entity.entity_name}")
                    else:
                        click.echo("      - (direct flow)")
        click.echo()

        # Test all configured connections
        connections = config.connections
        if connections:
            click.echo(f"🔌 Testing {len(connections)} database connection(s)...\n")

            for conn_name, conn_config in connections.items():
                click.echo(f"  {conn_name}")
                click.echo(f"    Type: {conn_config.get('type', 'unknown')}")
                click.echo(f"    Server: {conn_config.get('server', 'unknown')}")
                click.echo(f"    Database: {conn_config.get('database', 'unknown')}")

                try:
                    # Test the connection based on type
                    conn_type = conn_config.get("type", "").lower()

                    if conn_type == "mssql":
                        asyncio.run(_test_mssql_connection(conn_name, conn_config))
                    else:
                        click.echo(
                            f"    ⚠️  Connection type '{conn_type}' "
                            "not yet supported for testing"
                        )

                except Exception as e:
                    click.echo("    ❌ Connection failed")
                    click.echo(f"    Problem: {str(e)}")
                    click.echo("    💡 Try: Check your credentials and network access")
                click.echo()
        else:
            click.echo("ℹ️  No database connections configured (that's okay!)\n")

        # Add path validation for flows
        click.echo("📁 Validating paths...")
        _validate_flow_paths(config, workspace)

        # Success summary
        click.echo("\n✨ Everything looks good!")
        click.echo("   Your hygge project is ready to go.")
        click.echo("\n💡 Next steps:")
        click.echo("   • Run: hygge go")
        click.echo("   • Or run specific flows: hygge go --flow flow_name")

        logger.info("Project configuration debug completed")

    except ConfigError as e:
        click.echo("\n❌ Configuration Error")
        click.echo(f"   {e}")
        click.echo("\n💡 What to do:")
        click.echo("   • Check your hygge.yml and flow.yml files")
        click.echo("   • Make sure all required fields are present")
        click.echo("   • Verify YAML syntax is correct")
        sys.exit(1)
    except Exception as e:
        click.echo("\n❌ Unexpected Error")
        click.echo(f"   {e}")
        click.echo("\n💡 This might help:")
        click.echo("   • Check file permissions")
        click.echo("   • Verify you're in a hygge project directory")
        click.echo("   • Look for a hygge.yml file in the current or parent directory")
        logger.error(f"Error debugging configuration: {e}")
        sys.exit(1)


def _validate_flow_paths(config, workspace):
    """Validate that paths referenced in flows exist and are accessible."""
    from pathlib import Path

    issues_found = []
    paths_checked = 0

    for entity in config.entities:
        flow_config = entity.flow_config

        # Check home paths (for parquet homes)
        home = flow_config.home
        if isinstance(home, dict):
            home_type = home.get("type", "")
            if home_type == "parquet":
                home_path = home.get("path")
                if home_path:
                    paths_checked += 1
                    full_path = Path(workspace.root) / home_path
                    if not full_path.exists():
                        issues_found.append(
                            f"  ⚠️  Home path doesn't exist: {home_path}"
                        )
                        issues_found.append(f"     Flow: {entity.base_flow_name}")
                        issues_found.append(f"     💡 Create it: mkdir -p {home_path}")

        # Check store paths (for parquet stores)
        store = flow_config.store
        if isinstance(store, dict):
            store_type = store.get("type", "")
            if store_type == "parquet":
                store_path = store.get("path")
                if store_path:
                    paths_checked += 1
                    full_path = Path(workspace.root) / store_path
                    # For stores, check if parent directory exists
                    # (store path will be created)
                    parent = full_path.parent
                    if not parent.exists():
                        relative_parent = Path(store_path).parent
                        issues_found.append(
                            f"  ⚠️  Store parent directory "
                            f"doesn't exist: {relative_parent}"
                        )
                        issues_found.append(f"     Flow: {entity.base_flow_name}")
                        issues_found.append(
                            f"     💡 Create it: mkdir -p {relative_parent}"
                        )

    if issues_found:
        warning_count = len([i for i in issues_found if i.startswith("  ⚠️")])
        click.echo(f"  Found issues with {warning_count} path(s):")
        for issue in issues_found:
            click.echo(issue)
        click.echo()
    else:
        if paths_checked > 0:
            click.echo(f"  ✓ All {paths_checked} path(s) look good\n")
        else:
            click.echo("  ℹ️  No local paths to validate\n")


async def _test_mssql_connection(conn_name: str, conn_config: dict):
    """Test MSSQL connection with detailed feedback."""
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
        connection = await connection_factory.get_connection()

        # Run a simple test query
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test_value")
        cursor.fetchone()  # Just test the query works
        cursor.close()
        connection.close()

        click.echo("    ✓ Connection successful!")

    except Exception:
        raise


def _print_preview(preview_results: list, verbose: bool, flow_filter: Optional[list]):
    """Print dry-run preview using click formatting."""
    click.echo("\n🏡 hygge dry-run preview\n")

    # Header
    if flow_filter:
        filter_str = ", ".join(flow_filter)
        click.echo(f"Would run {len(preview_results)} flow(s) matching: {filter_str}")
    else:
        click.echo(f"Would run {len(preview_results)} flow(s)")
    click.echo("")

    # Show each flow
    warning_count = 0
    for result in preview_results:
        if verbose:
            _print_verbose_flow(result)
        else:
            _print_concise_flow(result)

        if result.get("warnings"):
            warning_count += 1

    # Footer
    click.echo("")
    click.echo("📊 Summary:")
    click.echo(f"   ✓ {len(preview_results)} flow(s) configured")
    if warning_count > 0:
        click.echo(f"   ⚠️  {warning_count} flow(s) with warnings")

    click.echo("")
    click.echo("💡 Next steps:")
    if verbose and warning_count > 0:
        click.echo("   • Review warnings above")
    click.echo("   • Test connections: hygge debug")
    if flow_filter:
        click.echo(f"   • Run flows: hygge go --flow {','.join(flow_filter)}")
    else:
        click.echo("   • Run flows: hygge go")


def _print_concise_flow(result: dict):
    """Print one-line flow preview."""
    # Required fields - fail fast if missing
    flow_name = result["flow_name"]
    home_info = result["home_info"]
    store_info = result["store_info"]
    home_type = home_info["type"]
    store_type = store_info["type"]

    # Optional fields - explicit defaults
    warnings = result.get("warnings", [])  # Optional: list of warnings
    # Optional: incremental config
    incremental_info = result.get("incremental_info", {})

    # Determine indicator and mode
    indicator = "⚠️ " if warnings else "✓"
    mode = "(incremental)" if incremental_info.get("enabled") else "(full load)"

    click.echo(f"{indicator} {flow_name:30} {home_type} → {store_type} {mode}")


def _print_verbose_flow(result: dict):
    """Print detailed flow preview."""
    # Required fields
    flow_name = result["flow_name"]
    home_info = result["home_info"]
    store_info = result["store_info"]

    # Optional fields - explicit
    entity_name = result.get("entity_name")  # Optional: entity-specific name
    base_flow_name = result.get("base_flow_name")  # Optional: base flow
    # Optional: incremental config
    incremental_info = result.get("incremental_info", {})
    warnings = result.get("warnings", [])  # Optional: list of warnings

    click.echo("━" * 60)
    if entity_name and entity_name != flow_name:
        click.echo(f"Flow: {base_flow_name}.{entity_name}")
    else:
        click.echo(f"Flow: {flow_name}")
    click.echo("━" * 60)
    click.echo("")

    # Source
    click.echo("📥 Source")
    click.echo(f"   Type: {home_info['type']}")
    if home_info.get("path"):
        click.echo(f"   Path: {home_info['path']}")
    if home_info.get("table"):
        click.echo(f"   Table: {home_info['table']}")
    if home_info.get("connection"):
        click.echo(f"   Connection: {home_info['connection']}")
    click.echo("")

    # Destination
    click.echo("📤 Destination")
    click.echo(f"   Type: {store_info['type']}")
    if store_info.get("path"):
        click.echo(f"   Path: {store_info['path']}")
    if store_info.get("table"):
        click.echo(f"   Table: {store_info['table']}")
    if store_info.get("workspace"):
        click.echo(f"   Workspace: {store_info['workspace']}")
    if store_info.get("lakehouse"):
        click.echo(f"   Lakehouse: {store_info['lakehouse']}")
    click.echo("")

    # Incremental
    if incremental_info.get("enabled"):
        click.echo("💧 Incremental Mode: Enabled")
        wm_col = incremental_info.get("watermark_column")
        if wm_col:
            click.echo(f"   Watermark column: {wm_col}")
            click.echo(f"   Would process rows where {wm_col} > last_watermark")
    else:
        click.echo("💧 Mode: Full load")
        click.echo("   Would process all rows")
    click.echo("")

    # Warnings
    if warnings:
        for warning in warnings:
            click.echo(f"⚠️  {warning}")
        click.echo("")

    click.echo("✓ Ready to preview")
    click.echo("   (use 'hygge debug' to test connections)")
    click.echo("")


if __name__ == "__main__":
    hygge()
