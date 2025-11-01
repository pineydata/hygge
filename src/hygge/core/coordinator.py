"""
Coordinator orchestrates data flows based on configuration.

The coordinator's single responsibility is to:
1. Read and parse configuration templates
2. Manage connection pools for database sources
3. Orchestrate flows in parallel
4. Handle flow-level error management

Home and Store instantiation is delegated to Flow.
"""
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

from hygge.connections import MSSQL_CONNECTION_DEFAULTS, ConnectionPool, MssqlConnection
from hygge.utility.exceptions import ConfigError
from hygge.utility.logger import get_logger

from .flow import Flow, FlowConfig
from .home import Home
from .store import Store

# Store-related configuration keys that can be applied as defaults
STORE_DEFAULT_KEYS = ["if_exists", "batch_size", "parallel_workers", "timeout"]


def validate_config(config: Dict[str, Any]) -> List[str]:
    """Validate configuration using Pydantic models."""
    try:
        CoordinatorConfig.from_dict(config)
        return []
    except Exception as e:
        return [str(e)]


class Coordinator:
    """
    Orchestrates multiple data flows based on configuration.

    The coordinator:
    - Reads configuration from YAML files
    - Validates configuration using Pydantic models
    - Orchestrates flows in parallel
    - Handles flow-level error management
    """

    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            # Project discovery mode - look for hygge.yml
            config_path = self._find_project_config()

        self.config_path = Path(config_path)
        self.config = None
        self.flows: List[Flow] = []
        self.options: Dict[str, Any] = {}
        self.project_config: Dict[str, Any] = {}
        self.connection_pools: Dict[str, ConnectionPool] = {}  # Named connection pools
        self.logger = get_logger("hygge.coordinator")

        # Load project config if we found hygge.yml
        self._load_project_config()

        # No longer need Factory - using registry pattern directly

    def _find_project_config(self) -> str:
        """Look for hygge.yml in current directory and parents."""
        current = Path.cwd()
        searched_paths = []

        while current != current.parent:
            project_file = current / "hygge.yml"
            searched_paths.append(str(project_file))
            if project_file.exists():
                return str(project_file)
            current = current.parent

        # dbt-style error message
        error_msg = f"""
No hygge.yml found in current path: {Path.cwd()}

Searched locations:
{chr(10).join(f"  - {path}" for path in searched_paths)}

To get started, run:
  hygge init <project_name>
"""
        raise ConfigError(error_msg)

    def _load_project_config(self) -> None:
        """Load project configuration from hygge.yml."""
        if self.config_path.name == "hygge.yml":
            # We found hygge.yml, load project config
            with open(self.config_path, "r") as f:
                raw_config = yaml.safe_load(f)
                self.project_config = raw_config or {}

            project_name = self.project_config.get("name", "unnamed")
            self.logger.info(f"Loaded project config: {project_name}")
            self.logger.debug(f"Raw project config: {raw_config}")
        else:
            # Legacy mode - no project config
            self.project_config = {}

        # Expand environment variables in project config
        self.project_config = self._expand_env_vars(self.project_config)

    def _expand_env_vars(self, data: Any) -> Any:
        """
        Recursively expand environment variables in configuration data.

        Supports patterns like ${VAR_NAME} and ${VAR_NAME:-default_value}
        """
        import os
        import re

        if isinstance(data, str):
            # Pattern: ${VAR_NAME} or ${VAR_NAME:-default}
            pattern = r"\$\{([^:}]+)(?::-([^}]*))?\}"

            def replace_env_var(match):
                var_name = match.group(1)
                default_value = match.group(2) if match.group(2) is not None else ""

                env_value = os.getenv(var_name)
                if env_value is not None:
                    return env_value
                elif default_value:
                    return default_value
                else:
                    raise ConfigError(
                        f"Environment variable '{var_name}' is not set and no default"
                    )

            return re.sub(pattern, replace_env_var, data)

        elif isinstance(data, dict):
            return {key: self._expand_env_vars(value) for key, value in data.items()}

        elif isinstance(data, list):
            return [self._expand_env_vars(item) for item in data]

        else:
            return data

    async def run(self) -> None:
        """Run all configured flows."""
        self.logger.info(f"Starting coordinator with config: {self.config_path}")

        try:
            # Load and validate configuration
            self._load_config()

            # Initialize connection pools
            await self._initialize_connection_pools()

            # Create flows
            self._create_flows()

            # Run flows in parallel
            await self._run_flows()

        finally:
            # Clean up connection pools
            await self._cleanup_connection_pools()

    def _load_config(self) -> None:
        """Load and validate configuration from file or directory."""
        try:
            if self.config_path.is_file():
                if self.config_path.name == "hygge.yml":
                    # Project-centric mode - load flows from flows/ directory
                    self._load_project_flows()
                else:
                    # Single file configuration (legacy)
                    self._load_single_file_config()
            elif self.config_path.is_dir():
                # Directory-based configuration (legacy progressive approach)
                self._load_directory_config()
            else:
                raise ConfigError(
                    f"Configuration path must be file or directory: {self.config_path}"
                )

        except Exception as e:
            raise ConfigError(f"Failed to load configuration: {str(e)}")

    def _load_project_flows(self) -> None:
        """Load flows from flows/ directory in project-centric mode."""
        # Get flows directory from project config
        flows_dir_name = self.project_config.get("flows_dir", "flows")
        flows_dir = self.config_path.parent / flows_dir_name

        self.logger.debug(f"Looking for flows in: {flows_dir}")
        self.logger.debug(f"Project config: {self.project_config}")

        if not flows_dir.exists():
            raise ConfigError(f"Flows directory not found: {flows_dir}")

        flows = {}

        # Look for flow directories
        for flow_dir in flows_dir.iterdir():
            self.logger.debug(f"Checking directory: {flow_dir}")
            if flow_dir.is_dir() and (flow_dir / "flow.yml").exists():
                flow_name = flow_dir.name
                try:
                    # Load flow config
                    flow_config = self._load_flow_config(flow_dir)
                    flows[flow_name] = flow_config
                    self.logger.debug(f"Loaded flow: {flow_name}")
                except Exception as e:
                    self.logger.error(f"Failed to load flow {flow_name}: {str(e)}")
                    raise ConfigError(f"Failed to load flow {flow_name}: {str(e)}")

        if not flows:
            raise ConfigError(f"No flows found in directory: {flows_dir}")

        # Create coordinator config with connections if present
        connections = self.project_config.get("connections", {})
        self.config = CoordinatorConfig(flows=flows, connections=connections)

        # Load global options from project config
        self.options = self.project_config.get("options", {})

        self.logger.info(
            f"Loaded project flows with {len(flows)} flows from {flows_dir}"
        )

    def _load_flow_config(self, flow_dir: Path) -> FlowConfig:
        """Load flow configuration from flow directory."""
        flow_file = flow_dir / "flow.yml"
        with open(flow_file, "r") as f:
            flow_data = yaml.safe_load(f)

        # Expand environment variables in flow config
        flow_data = self._expand_env_vars(flow_data)

        # Load entities if they exist
        entities_dir = flow_dir / "entities"
        if entities_dir.exists():
            entities = self._load_entities(entities_dir, flow_data.get("defaults", {}))
            flow_data["entities"] = entities

        return FlowConfig(**flow_data)

    def _load_entities(
        self, entities_dir: Path, defaults: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Load entity definitions from entities directory."""
        entities = []

        for entity_file in entities_dir.glob("*.yml"):
            with open(entity_file, "r") as f:
                entity_data = yaml.safe_load(f)

            # Expand environment variables in entity config
            entity_data = self._expand_env_vars(entity_data)

            # Merge with defaults
            entity_data = {**defaults, **entity_data}
            entities.append(entity_data)

        return entities

    def _load_single_file_config(self) -> None:
        """Load configuration from single YAML file (legacy approach)."""
        with open(self.config_path, "r") as f:
            config_data = yaml.safe_load(f)

        # Validate configuration
        errors = validate_config(config_data)
        if errors:
            raise ConfigError(f"Configuration validation failed: {errors}")

        # Parse with Pydantic
        self.config = CoordinatorConfig.from_dict(config_data)

        # Extract options
        self.options = config_data.get("options", {})

        self.logger.info(
            f"Loaded single-file configuration with {len(self.config.flows)} flows"
        )

    def _load_directory_config(self) -> None:
        """Load configuration from directory structure (progressive approach)."""
        flows = {}

        # Look for flow directories
        for flow_dir in self.config_path.iterdir():
            if flow_dir.is_dir() and (flow_dir / "flow.yml").exists():
                flow_name = flow_dir.name
                try:
                    # Load flow config directly
                    flow_file = flow_dir / "flow.yml"
                    with open(flow_file, "r") as f:
                        flow_data = yaml.safe_load(f)
                    flow_config = FlowConfig(**flow_data)
                    flows[flow_name] = flow_config
                    self.logger.debug(f"Loaded flow: {flow_name}")
                except Exception as e:
                    raise ConfigError(f"Failed to load flow {flow_name}: {str(e)}")

        if not flows:
            raise ConfigError(f"No flows found in directory: {self.config_path}")

        # Create coordinator config
        self.config = CoordinatorConfig(flows=flows)

        # Load global options if they exist
        options_file = self.config_path / "options.yml"
        if options_file.exists():
            with open(options_file, "r") as f:
                self.options = yaml.safe_load(f) or {}
        else:
            self.options = {}

        self.logger.info(f"Loaded directory configuration with {len(flows)} flows")

    async def _initialize_connection_pools(self) -> None:
        """Initialize connection pools from configuration."""
        if not self.config or not self.config.connections:
            self.logger.debug("No connections configured, skipping pool initialization")
            return

        num_conns = len(self.config.connections)
        self.logger.info(f"Initializing {num_conns} connection pools")

        for conn_name, conn_config in self.config.connections.items():
            try:
                # Create connection factory based on type
                conn_type = conn_config.get("type")

                if conn_type == "mssql":
                    # Use shared defaults to avoid duplication
                    defaults = MSSQL_CONNECTION_DEFAULTS
                    # Convert timeout to int if it's a string
                    timeout = conn_config.get("timeout", defaults.timeout)
                    if isinstance(timeout, str):
                        timeout = int(timeout)
                    factory = MssqlConnection(
                        server=conn_config.get("server"),
                        database=conn_config.get("database"),
                        options={
                            "driver": conn_config.get("driver", defaults.driver),
                            "encrypt": conn_config.get("encrypt", defaults.encrypt),
                            "trust_cert": conn_config.get(
                                "trust_cert", defaults.trust_cert
                            ),
                            "timeout": timeout,
                        },
                    )
                else:
                    raise ConfigError(f"Unknown connection type: {conn_type}")

                # Create pool
                pool_size = conn_config.get("pool_size", 5)
                # Convert to int if it's a string (from environment variables)
                if isinstance(pool_size, str):
                    pool_size = int(pool_size)
                pool = ConnectionPool(
                    name=conn_name, connection_factory=factory, pool_size=pool_size
                )

                # Initialize pool (pre-create connections)
                await pool.initialize()

                # Store pool
                self.connection_pools[conn_name] = pool

                self.logger.success(
                    f"Initialized pool '{conn_name}' with {pool_size} connections"
                )

            except Exception as e:
                raise ConfigError(
                    f"Failed to initialize connection pool '{conn_name}': {str(e)}"
                )

    async def _cleanup_connection_pools(self) -> None:
        """Clean up all connection pools."""
        if not self.connection_pools:
            return

        num_pools = len(self.connection_pools)
        self.logger.info(f"Cleaning up {num_pools} connection pools")

        for pool_name, pool in self.connection_pools.items():
            try:
                await pool.close()
                self.logger.debug(f"Closed pool '{pool_name}'")
            except Exception as e:
                self.logger.warning(f"Error closing pool '{pool_name}': {str(e)}")

        self.connection_pools.clear()
        self.logger.success("All connection pools cleaned up")

    def _inject_store_pool(self, store: Store, store_config) -> None:
        """
        Inject connection pool into stores that need it (e.g., MSSQL).

        Args:
            store: Store instance to inject pool into
            store_config: Store configuration with connection reference
        """
        if not hasattr(store, "set_pool"):
            return

        if not hasattr(store_config, "connection"):
            return

        if not store_config.connection:
            return

        pool = self.connection_pools.get(store_config.connection)
        if pool:
            store.set_pool(pool)
        else:
            conn_name = store_config.connection
            self.logger.warning(f"Connection '{conn_name}' referenced but not found")

    def _create_flows(self) -> None:
        """Create Flow instances from configuration."""
        self.flows = []

        for flow_name, flow_config in self.config.flows.items():
            try:
                # Check if entities are defined
                if flow_config.entities and len(flow_config.entities) > 0:
                    # Create one flow per entity
                    num_entities = len(flow_config.entities)
                    self.logger.debug(
                        f"Creating flows for {num_entities} entities in {flow_name}"
                    )
                    for entity in flow_config.entities:
                        # Handle simple string entities (landing zone pattern)
                        if isinstance(entity, str):
                            entity_name = entity
                        # Handle dict entities (project-centric pattern)
                        elif isinstance(entity, dict):
                            entity_name = entity.get("name")
                            if not entity_name:
                                raise ConfigError(
                                    f"Entity in flow {flow_name} missing 'name' field"
                                )
                        else:
                            raise ConfigError(
                                f"Entity must be string or dict, got {type(entity)}"
                            )

                        # Create entity-specific flow
                        entity_flow_name = f"{flow_name}_{entity_name}"
                        self._create_entity_flow(
                            entity_flow_name, flow_config, entity_name, entity
                        )
                else:
                    # Create single flow without entities
                    home = flow_config.home_instance
                    store = flow_config.store_instance

                    # Inject connection pool into stores that need it
                    self._inject_store_pool(store, store.config)

                    # Create flow with options
                    flow_options = flow_config.options.copy()
                    flow_options.update(
                        {
                            "queue_size": flow_config.queue_size,
                            "timeout": flow_config.timeout,
                        }
                    )

                    flow = Flow(flow_name, home, store, flow_options)
                    self.flows.append(flow)

                    self.logger.debug(f"Created flow: {flow_name}")

            except Exception as e:
                raise ConfigError(f"Failed to create flow {flow_name}: {str(e)}")

    def _create_entity_flow(
        self,
        flow_name: str,
        flow_config: FlowConfig,
        entity_name: str,
        entity_config: dict,
    ) -> None:
        """Create a flow for a specific entity with entity subdirectories."""
        # Get the original config from home/store instances
        home_config = flow_config.home_config
        store_config = flow_config.store_config

        if not home_config or not store_config:
            raise ConfigError(
                "Cannot create entity flow: home/store configs not accessible"
            )

        # Merge entity configuration with flow configuration
        if isinstance(entity_config, dict):
            # Merge entity home config with flow home config
            if "home" in entity_config:
                entity_home_config = entity_config["home"]
                # Create new home config with entity overrides
                home_config_dict = (
                    home_config.model_dump()
                    if hasattr(home_config, "model_dump")
                    else home_config.__dict__
                )

                # Special handling for path merging - append entity path to flow path
                if "path" in entity_home_config and "path" in home_config_dict:
                    flow_path = home_config_dict["path"]
                    entity_path = entity_home_config["path"]
                    # Combine paths properly
                    merged_path = f"{flow_path.rstrip('/')}/{entity_path.lstrip('/')}"
                    merged_home_config = {
                        **home_config_dict,
                        **entity_home_config,
                        "path": merged_path,
                    }
                else:
                    merged_home_config = {**home_config_dict, **entity_home_config}

                home_config = type(home_config)(**merged_home_config)

            # Merge entity store config with flow store config
            if "store" in entity_config:
                entity_store_config = entity_config["store"]
                # Create new store config with entity overrides
                store_config_dict = (
                    store_config.model_dump()
                    if hasattr(store_config, "model_dump")
                    else store_config.__dict__
                )
                merged_store_config = {**store_config_dict, **entity_store_config}
                store_config = type(store_config)(**merged_store_config)

        # Apply flow defaults to store config
        # The defaults are already merged into entity_config in _load_entities()
        # We need to extract store-related defaults and apply them
        store_defaults = {}
        if isinstance(entity_config, dict):
            # Extract store-related defaults that might be at the entity level
            for key in STORE_DEFAULT_KEYS:
                if key in entity_config:
                    store_defaults[key] = entity_config[key]

        if store_defaults:
            store_config_dict = (
                store_config.model_dump()
                if hasattr(store_config, "model_dump")
                else store_config.__dict__
            )
            # Merge defaults into store config
            merged_store_config = {**store_config_dict, **store_defaults}
            store_config = type(store_config)(**merged_store_config)

        # Get pool if home references a named connection
        pool = None
        if hasattr(home_config, "connection") and home_config.connection:
            pool = self.connection_pools.get(home_config.connection)
            if pool is None and home_config.connection is not None:
                conn_name = home_config.connection
                self.logger.warning(
                    f"Connection '{conn_name}' referenced but not found"
                )

        # Create new home and store instances with entity_name
        # Pass pool to home if it's an MssqlHome
        from hygge.homes.mssql import MssqlHome

        if home_config.type == "mssql":
            home = MssqlHome(
                f"{flow_name}_home", home_config, pool=pool, entity_name=entity_name
            )
        else:
            # For parquet homes, don't pass entity_name if entity config specifies path
            # This prevents double path appending
            if (
                isinstance(entity_config, dict)
                and "home" in entity_config
                and "path" in entity_config["home"]
            ):
                home = Home.create(f"{flow_name}_home", home_config)
            else:
                home = Home.create(f"{flow_name}_home", home_config, entity_name)

        store = Store.create(f"{flow_name}_store", store_config, flow_name, entity_name)

        # Inject connection pool into stores that need it
        self._inject_store_pool(store, store_config)

        # Create flow with options
        flow_options = flow_config.options.copy()
        flow_options.update(
            {
                "queue_size": flow_config.queue_size,
                "timeout": flow_config.timeout,
            }
        )

        flow = Flow(flow_name, home, store, flow_options)
        self.flows.append(flow)

        self.logger.debug(f"Created entity flow: {flow_name} for entity: {entity_name}")

    async def _run_flows(self) -> None:
        """Run all flows in parallel."""
        if not self.flows:
            self.logger.warning("No flows to run")
            return

        self.logger.info(f"Running {len(self.flows)} flows in parallel")

        # Create tasks for all flows
        tasks = []
        for flow in self.flows:
            task = asyncio.create_task(self._run_flow(flow), name=f"flow_{flow.name}")
            tasks.append(task)

        # Run all flows concurrently
        try:
            await asyncio.gather(*tasks)
            self.logger.success("All flows completed successfully")
        except Exception as e:
            self.logger.error(f"Some flows failed: {str(e)}")
            # Cancel remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
            raise

    async def _run_flow(self, flow: Flow) -> None:
        """Run a single flow with error handling."""
        try:
            await flow.start()
        except Exception as e:
            self.logger.error(f"Error in flow {flow.name}: {e}")
            if not self.options.get("continue_on_error", False):
                raise


class CoordinatorConfig(BaseModel):
    """Main configuration model for hygge."""

    flows: Dict[str, FlowConfig] = Field(..., description="Flow configurations")
    connections: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Named connection pool configurations"
    )

    @field_validator("flows")
    @classmethod
    def validate_flows_not_empty(cls, v):
        """Validate flows section is not empty."""
        if not v:
            raise ValueError("At least one flow must be configured")
        return v

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CoordinatorConfig":
        """Create configuration from dictionary."""
        return cls(**data)

    def get_flow_config(self, flow_name: str) -> FlowConfig:
        """Get configuration for a specific flow."""
        if flow_name not in self.flows:
            raise ValueError(f"Flow '{flow_name}' not found in configuration")
        return self.flows[flow_name]
