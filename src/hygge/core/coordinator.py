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
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator

from hygge.connections import MSSQL_CONNECTION_DEFAULTS, ConnectionPool, MssqlConnection
from hygge.utility.exceptions import ConfigError
from hygge.utility.logger import get_logger
from hygge.utility.path_helper import PathHelper
from hygge.utility.run_id import generate_run_id

from .flow import Flow, FlowConfig
from .home import Home
from .journal import Journal, JournalConfig
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

    def __init__(
        self,
        config_path: Optional[str] = None,
        flow_overrides: Optional[Dict[str, Any]] = None,
        flow_filter: Optional[List[str]] = None,
    ):
        if config_path is None:
            # Project discovery mode - look for hygge.yml
            config_path = self._find_project_config()

        self.config_path = Path(config_path)
        self.config = None
        self.flows: List[Flow] = []
        self.options: Dict[str, Any] = {}
        self.project_config: Dict[str, Any] = {}
        self.connection_pools: Dict[str, ConnectionPool] = {}  # Named connection pools
        self.flow_overrides = flow_overrides or {}  # CLI overrides for flow configs
        self.flow_filter = flow_filter or []  # List of flow names to execute
        self.logger = get_logger("hygge.coordinator")

        # Flow results tracking for dbt-style summary
        self.flow_results: List[Dict[str, Any]] = []
        self.run_start_time: Optional[float] = None

        # Load project config if we found hygge.yml
        self._load_project_config()

        # No longer need Factory - using registry pattern directly
        self.coordinator_name = self._resolve_coordinator_name()
        self.coordinator_run_id: Optional[str] = None
        self.coordinator_start_time: Optional[datetime] = None
        self.journal_config: Optional[JournalConfig] = None
        self.journal: Optional[Journal] = None
        self._journal_cache: Dict[str, Journal] = {}
        self.flow_run_ids: Dict[str, str] = {}

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

    def _resolve_coordinator_name(self) -> str:
        """Determine coordinator name from project config or config path."""
        if self.project_config and self.project_config.get("name"):
            return str(self.project_config["name"])
        if self.config_path:
            stem = self.config_path.stem
            if stem:
                return stem
        return "coordinator"

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
        self.coordinator_start_time = datetime.now(timezone.utc)
        coordinator_start_iso = self.coordinator_start_time.isoformat()
        self.coordinator_run_id = generate_run_id(
            [self.coordinator_name, coordinator_start_iso]
        )
        self.flow_run_ids = {}

        try:
            # Load and validate configuration
            self._load_config()

            # Initialize connection pools
            await self._initialize_connection_pools()

            # Create flows
            self._create_flows()

            # Validate that at least one flow was created (if filter specified)
            if self.flow_filter and len(self.flows) == 0:
                available_flows = list(self.config.flows.keys())
                # Also collect entity flow names
                entity_flows = []
                for base_flow_name, flow_config in self.config.flows.items():
                    if flow_config.entities:
                        for entity in flow_config.entities:
                            if isinstance(entity, str):
                                entity_name = entity
                            elif isinstance(entity, dict):
                                entity_name = entity.get("name")
                            else:
                                continue
                            if entity_name:
                                entity_flows.append(f"{base_flow_name}_{entity_name}")

                entity_flows_str = ", ".join(entity_flows) if entity_flows else "none"
                raise ConfigError(
                    f"No flows matched filter: {', '.join(self.flow_filter)}. "
                    f"Available flows: {', '.join(available_flows)}. "
                    f"Available entity flows: {entity_flows_str}."
                )

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

        if self.config:
            self.journal_config = (
                self.config.journal
                if isinstance(self.config.journal, JournalConfig)
                else None
            )

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
        journal_config = self.project_config.get("journal")
        self.config = CoordinatorConfig(
            flows=flows, connections=connections, journal=journal_config
        )
        self.journal_config = (
            self.config.journal
            if isinstance(self.config.journal, JournalConfig)
            else None
        )

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
        self.journal_config = (
            self.config.journal
            if isinstance(self.config.journal, JournalConfig)
            else None
        )

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
        self.config = CoordinatorConfig(flows=flows, journal=None)
        self.journal_config = None

        # Load global options if they exist
        options_file = self.config_path / "options.yml"
        if options_file.exists():
            with open(options_file, "r") as f:
                self.options = yaml.safe_load(f) or {}
        else:
            self.options = {}

        self.logger.info(f"Loaded directory configuration with {len(flows)} flows")

    async def _initialize_connection_pools(self) -> None:
        """Initialize connection pools and execution engines from configuration."""
        # Initialize ThreadPoolEngine for MSSQL (and future synchronous DBs)
        # Use default pool size of 8, or match max connection pool size if configured
        from hygge.connections import ThreadPoolEngine

        max_pool_size = 8  # Default
        if self.config and self.config.connections:
            # Use the largest pool size as a hint for thread pool size
            # int() handles strings, floats, and integers gracefully
            max_pool_size = max(
                (
                    int(conn.get("pool_size", 5))
                    for conn in self.config.connections.values()
                ),
                default=8,
            )

        ThreadPoolEngine.initialize(pool_size=max_pool_size)
        self.logger.debug(f"Initialized ThreadPoolEngine with {max_pool_size} workers")

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
        """Clean up all connection pools and execution engines."""
        # Shutdown ThreadPoolEngine
        from hygge.connections import ThreadPoolEngine

        if ThreadPoolEngine.is_initialized():
            ThreadPoolEngine.shutdown()

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

    def _get_or_create_journal_instance(
        self,
        flow_level_config: Optional[JournalConfig],
        store_config,
        home_config,
        store,
    ) -> Optional[Journal]:
        """
        Resolve journal instance for a flow/entity based on configuration.

        Args:
            flow_level_config: Journal config defined at the flow/entity level.
            store_config: Store configuration (for path inference).
            home_config: Home configuration (for path inference).

        Returns:
            Journal instance or None if journaling is disabled.
        """
        effective_config = flow_level_config or self.journal_config
        if not effective_config:
            return None

        cache_key = json.dumps(effective_config.model_dump(mode="json"), sort_keys=True)
        journal = self._journal_cache.get(cache_key)

        if not journal:
            store_path = getattr(store_config, "path", None)
            home_path = getattr(home_config, "path", None)

            journal = Journal(
                name=self.coordinator_name,
                config=effective_config,
                coordinator_name=self.coordinator_name,
                store_path=str(store_path) if store_path else None,
                home_path=str(home_path) if home_path else None,
                store=store,
                store_config=store_config,
                home_config=home_config,
            )
            self._journal_cache[cache_key] = journal

            if (
                flow_level_config is None
                and self.journal is None
                and effective_config is self.journal_config
            ):
                self.journal = journal

        return journal

    def _create_flows(self) -> None:
        """Create Flow instances from configuration."""
        self.flows = []

        for base_flow_name, flow_config in self.config.flows.items():
            try:
                # If flow filter specified, check if this flow should be included
                if self.flow_filter and not self._should_include_flow(
                    base_flow_name, flow_config
                ):
                    continue

                # Apply CLI flow-level overrides (use base flow name, not entity name)
                # Update the config in place so tests can verify it
                if self.flow_overrides:
                    flow_config = self._apply_flow_overrides(
                        flow_config, base_flow_name
                    )
                    # Update config in place so it's accessible later
                    self.config.flows[base_flow_name] = flow_config

                default_run_type = flow_config.run_type or "full_drop"
                if flow_config.full_drop is not None:
                    default_run_type = (
                        "full_drop" if flow_config.full_drop else "incremental"
                    )
                default_watermark = flow_config.watermark
                flow_journal_config = (
                    flow_config.journal
                    if isinstance(flow_config.journal, JournalConfig)
                    else None
                )

                # Check if entities are defined
                if flow_config.entities and len(flow_config.entities) > 0:
                    # Create one flow per entity
                    num_entities = len(flow_config.entities)
                    self.logger.debug(
                        f"Creating flows for {num_entities} entities "
                        f"in {base_flow_name}"
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
                                    f"Entity in flow {base_flow_name} "
                                    f"missing 'name' field"
                                )
                        else:
                            raise ConfigError(
                                f"Entity must be string or dict, got {type(entity)}"
                            )

                        # Create entity-specific flow
                        # Note: entity flows use base_flow_name for overrides
                        entity_flow_name = f"{base_flow_name}_{entity_name}"

                        # If flow filter is specified, check if this specific entity
                        # flow should be included
                        if self.flow_filter:
                            # If base flow name is in filter, include all entities.
                            # Otherwise, only include if this specific entity flow
                            # name matches
                            if (
                                base_flow_name not in self.flow_filter
                                and entity_flow_name not in self.flow_filter
                            ):
                                continue

                        self._create_entity_flow(
                            entity_flow_name,
                            flow_config,
                            entity_name,
                            entity,
                            base_flow_name,
                            flow_journal_config,
                            default_run_type,
                            default_watermark,
                        )
                else:
                    # Create single flow without entities
                    home = flow_config.home_instance
                    home_config = home.config

                    store_config = flow_config.store_config

                    store = Store.create(
                        f"{base_flow_name}_store", store_config, base_flow_name, None
                    )

                    # Validate run_type and store incremental alignment
                    self._validate_run_type_incremental_alignment(
                        store_config, default_run_type, base_flow_name, None
                    )

                    # Inject connection pool into stores that need it
                    self._inject_store_pool(store, store_config)

                    journal_instance = self._get_or_create_journal_instance(
                        flow_journal_config, store_config, home_config, store
                    )

                    # Create flow with options
                    flow_options = flow_config.options.copy()
                    flow_options.update(
                        {
                            "queue_size": flow_config.queue_size,
                            "timeout": flow_config.timeout,
                        }
                    )

                    flow = Flow(
                        base_flow_name,
                        home,
                        store,
                        flow_options,
                        journal=journal_instance,
                        coordinator_run_id=self.coordinator_run_id,
                        flow_run_id=None,
                        coordinator_name=self.coordinator_name,
                        base_flow_name=base_flow_name,
                        entity_name=None,
                        run_type=default_run_type,
                        watermark_config=default_watermark,
                    )
                    self.flows.append(flow)

                    self.logger.debug(f"Created flow: {base_flow_name}")

            except Exception as e:
                raise ConfigError(f"Failed to create flow {base_flow_name}: {str(e)}")

    def _create_entity_flow(
        self,
        flow_name: str,
        flow_config: FlowConfig,
        entity_name: str,
        entity_config: Union[Dict[str, Any], str],
        base_flow_name: str,
        flow_journal_config: Optional[JournalConfig],
        default_run_type: str,
        default_watermark: Optional[Dict[str, str]],
    ) -> None:
        """Create a flow for a specific entity with entity subdirectories."""
        # Get the original config from home/store instances
        home_config = flow_config.home_config
        store_config = flow_config.store_config

        if not home_config or not store_config:
            raise ConfigError(
                "Cannot create entity flow: home/store configs not accessible"
            )

        entity_run_type = default_run_type
        entity_watermark = default_watermark
        entity_journal_config = flow_journal_config

        if isinstance(entity_config, dict):
            if "run_type" in entity_config and entity_config["run_type"]:
                entity_run_type = entity_config["run_type"]
            if "watermark" in entity_config and entity_config["watermark"]:
                entity_watermark = entity_config["watermark"]
            if "journal" in entity_config and entity_config["journal"]:
                raw_journal = entity_config["journal"]
                if isinstance(raw_journal, JournalConfig):
                    entity_journal_config = raw_journal
                elif isinstance(raw_journal, dict):
                    entity_journal_config = JournalConfig(**raw_journal)
                else:
                    raise ConfigError(
                        f"Invalid journal configuration for entity {entity_name}"
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
                    # Combine paths properly using PathHelper
                    merged_path = PathHelper.merge_paths(flow_path, entity_path)
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

        # Validate run_type and store incremental alignment
        self._validate_run_type_incremental_alignment(
            store_config, entity_run_type, base_flow_name, entity_name
        )

        # Inject connection pool into stores that need it
        self._inject_store_pool(store, store_config)

        journal_instance = self._get_or_create_journal_instance(
            entity_journal_config, store_config, home_config, store
        )

        # Create flow with options
        flow_options = flow_config.options.copy()
        flow_options.update(
            {
                "queue_size": flow_config.queue_size,
                "timeout": flow_config.timeout,
            }
        )

        flow = Flow(
            flow_name,
            home,
            store,
            flow_options,
            journal=journal_instance,
            coordinator_run_id=self.coordinator_run_id,
            flow_run_id=None,
            coordinator_name=self.coordinator_name,
            base_flow_name=base_flow_name,
            entity_name=entity_name,
            run_type=entity_run_type,
            watermark_config=entity_watermark,
        )
        self.flows.append(flow)

        self.logger.debug(f"Created entity flow: {flow_name} for entity: {entity_name}")

    async def _run_flows(self) -> None:
        """Run all flows in parallel with dbt-style logging and concurrency limiting."""
        if not self.flows:
            self.logger.warning("No flows to run")
            return

        # Reset flow results for this run
        self.flow_results = []
        self.run_start_time = asyncio.get_event_loop().time()

        # Progress tracking for coordinator-level milestones
        self.total_rows_progress = 0
        self.last_milestone_rows = 0
        self.milestone_interval = 1_000_000  # Log every 1M rows
        self.milestone_lock = asyncio.Lock()

        # Determine max concurrent flows
        # Limits how many flows run concurrently using a semaphore
        # to prevent connection contention.
        max_concurrent = self.options.get("concurrency", None)
        if max_concurrent is None:
            # Try to match pool size if we have connection pools
            if self.connection_pools:
                # Use the largest pool size as a hint
                max_concurrent = max(
                    (pool.size for pool in self.connection_pools.values()),
                    default=8,
                )
            else:
                max_concurrent = 8

        # Convert to int if it's a string (from environment variables)
        if isinstance(max_concurrent, str):
            max_concurrent = int(max_concurrent)

        self.logger.info(
            f"Running {len(self.flows)} flows with max concurrency of {max_concurrent}"
        )

        # Create semaphore to limit concurrent flow execution
        semaphore = asyncio.Semaphore(max_concurrent)

        # Log summary of flows starting (condensed to reduce noise)
        total_flows = len(self.flows)
        if total_flows <= 10:
            # For small numbers, show individual flows
            for i, flow in enumerate(self.flows, 1):
                self.logger.info(
                    f"[{i} of {total_flows}] STARTING flow {flow.name}",
                    color_prefix="START",
                )
        else:
            # For large numbers, just show summary
            self.logger.info(
                f"Starting {total_flows} flows with max concurrency "
                f"of {max_concurrent}",
                color_prefix="START",
            )

        # Create tasks for all flows with progress callback
        tasks = []
        for i, flow in enumerate(self.flows, 1):
            # Set progress callback so flow can report row updates
            # Pass flow number (use default arg to capture value, not reference)
            async def progress_callback(rows, flow_num=i):
                await self._update_progress(rows, flow_num)

            flow.set_progress_callback(progress_callback)
            task = asyncio.create_task(
                self._run_flow_with_semaphore(flow, i, total_flows, semaphore),
                name=f"flow_{flow.name}",
            )
            tasks.append(task)

        # Run all flows concurrently (but limited by semaphore)
        # Use return_exceptions=True so all flows complete even if some fail,
        # allowing us to generate a complete summary
        await asyncio.gather(*tasks, return_exceptions=True)

        # Generate and log dbt-style summary BEFORE checking for failures
        # This ensures the summary is always shown, even when flows fail
        self._log_summary()

        # Check for failed flows based on continue_on_error setting
        # Since _run_flow no longer re-raises, we check flow_results instead
        failed_flows = [r for r in self.flow_results if r.get("status") == "fail"]
        if failed_flows and not self.options.get("continue_on_error", False):
            # At least one flow failed and we're not continuing on error
            # Re-raise the first exception that was stored
            raise failed_flows[0]["_exception"]

    async def _run_flow_with_semaphore(
        self,
        flow: Flow,
        flow_num: int,
        total_flows: int,
        semaphore: asyncio.Semaphore,
    ) -> None:
        """Limit concurrent execution using a semaphore."""
        async with semaphore:
            await self._run_flow(flow, flow_num, total_flows)

    async def _run_flow(self, flow: Flow, flow_num: int, total_flows: int) -> None:
        """Run a single flow with error handling and hygge-style logging."""
        flow_result = {
            "name": flow.name,
            "status": None,  # "pass", "fail", "skip"
            "rows": 0,
            "duration": 0.0,
            "error": None,
        }

        # Ensure flow has current journal context and run IDs
        flow.coordinator_run_id = self.coordinator_run_id
        flow.coordinator_name = self.coordinator_name

        base_flow = flow.base_flow_name or flow.name
        if base_flow not in self.flow_run_ids:
            flow_start_time = datetime.now(timezone.utc)
            self.flow_run_ids[base_flow] = generate_run_id(
                [self.coordinator_name, base_flow, flow_start_time.isoformat()]
            )
        flow.flow_run_id = self.flow_run_ids[base_flow]

        try:
            await flow.start()

            # Determine status
            if flow.total_rows == 0:
                flow_result["status"] = "skip"
                flow_result["rows"] = 0
                flow_result["duration"] = flow.duration
                # Log SKIP line (hygge-style, yellow/warning)
                self.logger.warning(
                    f"[{flow_num} of {total_flows}] SKIPPED flow {flow.name} "
                    f".... (no rows)"
                )
            else:
                flow_result["status"] = "pass"
                flow_result["rows"] = flow.total_rows
                flow_result["duration"] = flow.duration
                # Log FINISHED line (hygge-style, green) - flow name, duration, and rows
                self.logger.info(
                    f"[{flow_num} of {total_flows}] FINISHED flow {flow.name} "
                    f"completed in {flow.duration:.1f}s ({flow.total_rows:,} rows)",
                    color_prefix="OK",
                )

        except Exception as e:
            flow_result["status"] = "fail"
            flow_result["duration"] = flow.duration if flow.duration > 0 else 0.0
            flow_result["error"] = str(e)

            # Log FAILED line (hygge-style, red)
            self.logger.error(
                f"[{flow_num} of {total_flows}] FAILED flow {flow.name} ...."
            )

            # Store exception for re-raising if needed
            flow_result["_exception"] = e

        # Track result for summary (always, even if we're about to raise)
        self.flow_results.append(flow_result)

        # Don't re-raise here - let _run_flows handle exception propagation
        # based on continue_on_error setting after all flows complete

    async def _update_progress(self, rows: int, flow_num: int) -> None:
        """Update coordinator-level progress tracking (called by flows)."""
        async with self.milestone_lock:
            self.total_rows_progress += rows
            current_total = self.total_rows_progress

            # Check if we've crossed any milestones since last log
            # Log at each 1M mark (1M, 2M, 3M, etc.)
            while current_total >= self.last_milestone_rows + self.milestone_interval:
                self.last_milestone_rows += self.milestone_interval
                milestone = self.last_milestone_rows

                elapsed = (
                    asyncio.get_event_loop().time() - self.run_start_time
                    if self.run_start_time
                    else 0.0
                )
                if elapsed > 0:
                    rate = milestone / elapsed
                    self.logger.info(
                        f"PROCESSED {milestone:,} rows in {elapsed:.1f}s "
                        f"({rate:,.0f} rows/s)"
                    )

    def _log_summary(self) -> None:
        """Log dbt-style summary after all flows complete."""
        if not self.flow_results:
            return

        elapsed_time = (
            asyncio.get_event_loop().time() - self.run_start_time
            if self.run_start_time
            else 0.0
        )

        total_rows = sum(r["rows"] for r in self.flow_results)
        passed = sum(1 for r in self.flow_results if r["status"] == "pass")
        failed = sum(1 for r in self.flow_results if r["status"] == "fail")
        skipped = sum(1 for r in self.flow_results if r["status"] == "skip")

        # dbt-style summary - clean and information-dense
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = elapsed_time % 60

        # Build time string conditionally based on non-zero units
        time_parts = []
        if hours > 0:
            time_parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            time_parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        # Always include seconds
        time_parts.append(f"{seconds:.2f} second{'s' if seconds != 1.0 else ''}")

        if len(time_parts) > 1:
            time_str = ", ".join(time_parts[:-1]) + f" and {time_parts[-1]}"
        else:
            time_str = time_parts[0]

        # Add cozy spacing
        self.logger.info("")

        # dbt-style summary line
        self.logger.info(
            f"Finished running {len(self.flow_results)} flows "
            f"in {time_str} ({elapsed_time:.2f}s)."
        )

        # Final status line (green if all pass, red if failures)
        if failed == 0:
            self.logger.info("Completed successfully", color_prefix="OK")
        else:
            self.logger.error("Completed with errors")

        # dbt-style status summary
        self.logger.info(
            f"Done. PASS={passed} WARN=0 ERROR={failed} SKIP={skipped} "
            f"TOTAL={len(self.flow_results)}"
        )

        # Optional: Show total rows processed
        if total_rows > 0:
            self.logger.info(f"Total rows processed: {total_rows:,}")
            if elapsed_time > 0:
                rate = total_rows / elapsed_time
                self.logger.info(f"Overall rate: {rate:,.0f} rows/s")

        # Add cozy spacing at end
        self.logger.info("")

        # Show failed flow details
        if failed > 0:
            self.logger.error("Failed flows:")
            for flow_result in self.flow_results:
                if flow_result["status"] == "fail":
                    error_msg = flow_result.get("error", "Unknown error")
                    self.logger.error(f"  {flow_result['name']}: {error_msg}")

    def _should_include_flow(
        self, base_flow_name: str, flow_config: FlowConfig
    ) -> bool:
        """
        Check if a flow should be included based on flow filter.

        Supports:
        - Exact base flow name match (e.g., "salesforce")
        - Exact entity flow name match (e.g., "salesforce_Involvement")
        - Base flow name match includes all entities
          (e.g., "salesforce" matches all salesforce_* flows)

        Args:
            base_flow_name: Base flow name from config
            flow_config: Flow configuration

        Returns:
            True if flow should be included, False otherwise
        """
        if not self.flow_filter:
            return True

        # Check if base flow name matches
        if base_flow_name in self.flow_filter:
            return True

        # Check if any entity flow name would match
        if flow_config.entities and len(flow_config.entities) > 0:
            for entity in flow_config.entities:
                # Handle string entities
                if isinstance(entity, str):
                    entity_name = entity
                # Handle dict entities
                elif isinstance(entity, dict):
                    entity_name = entity.get("name")
                    if not entity_name:
                        continue
                else:
                    continue

                # Check if entity flow name matches
                entity_flow_name = f"{base_flow_name}_{entity_name}"
                if entity_flow_name in self.flow_filter:
                    return True

        return False

    def _validate_run_type_incremental_alignment(
        self,
        store_config: Any,
        run_type: str,
        flow_name: str,
        entity_name: Optional[str] = None,
    ) -> None:
        """
        Validate that Flow run_type and store incremental behavior align.

        Warns when Flow run_type and store incremental override diverge,
        as this can lead to confusing behavior where the flow thinks it's
        running incrementally but the store is configured to truncate.

        Args:
            store_config: Store configuration object
            run_type: Flow run_type ('incremental' or 'full_drop')
            flow_name: Name of the flow
            entity_name: Optional entity name (for entity flows)
        """
        # Only validate ADLS-family stores (ADLS, OneLake, OpenMirroring)
        # They have the incremental field
        if not hasattr(store_config, "incremental"):
            return

        incremental_override = getattr(store_config, "incremental", None)
        if incremental_override is None:
            # No override set, so store defers to flow run_type - this is fine
            return

        # Determine expected behavior from run_type
        is_incremental_from_run_type = run_type != "full_drop"

        # Check if store override matches flow run_type
        if incremental_override != is_incremental_from_run_type:
            entity_str = f" (entity: {entity_name})" if entity_name else ""
            flow_display = f"{flow_name}{entity_str}"

            if incremental_override:
                # Store forces incremental, but flow is full_drop
                self.logger.warning(
                    f"Flow {flow_display} has run_type='{run_type}' (full reload), "
                    f"but store has incremental=True (append mode). "
                    f"Store will append new data instead of truncating. "
                    f"Consider setting store.incremental=False or "
                    f"flow run_type='incremental'."
                )
            else:
                # Store forces full_drop, but flow is incremental
                self.logger.warning(
                    f"Flow {flow_display} has run_type='{run_type}' (incremental), "
                    f"but store has incremental=False (truncate mode). "
                    f"Store will truncate destination before writing new data. "
                    f"Consider setting store.incremental=True or "
                    f"flow run_type='full_drop'."
                )

    def _apply_flow_overrides(
        self, flow_config: FlowConfig, flow_name: str
    ) -> FlowConfig:
        """
        Apply CLI flow-level overrides to flow configuration.

        Args:
            flow_config: FlowConfig to override
            flow_name: Name of the flow

        Returns:
            Updated FlowConfig with overrides applied
        """
        if not self.flow_overrides or flow_name not in self.flow_overrides:
            return flow_config

        overrides = self.flow_overrides[flow_name]
        if not overrides:
            return flow_config

        # Convert to dict, apply overrides, recreate
        config_dict = flow_config.model_dump()

        # Deep merge overrides
        def deep_merge(base: dict, override: dict):
            for key, value in override.items():
                if (
                    key in base
                    and isinstance(base[key], dict)
                    and isinstance(value, dict)
                ):
                    deep_merge(base[key], value)
                else:
                    base[key] = value

        deep_merge(config_dict, overrides)
        return FlowConfig(**config_dict)


class CoordinatorConfig(BaseModel):
    """Main configuration model for hygge."""

    flows: Dict[str, FlowConfig] = Field(..., description="Flow configurations")
    connections: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Named connection pool configurations"
    )
    journal: Optional[Union[Dict[str, Any], JournalConfig]] = Field(
        default=None,
        description="Journal configuration for tracking execution metadata",
    )

    @field_validator("flows")
    @classmethod
    def validate_flows_not_empty(cls, v):
        """Validate flows section is not empty."""
        if not v:
            raise ValueError("At least one flow must be configured")
        return v

    @field_validator("journal", mode="before")
    @classmethod
    def validate_journal(cls, v):
        """Validate and normalize journal configuration."""
        if v is None:
            return None
        if isinstance(v, JournalConfig):
            return v
        if isinstance(v, dict):
            return JournalConfig(**v)
        raise ValueError(
            "Journal configuration must be a dict or JournalConfig instance"
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CoordinatorConfig":
        """Create configuration from dictionary."""
        return cls(**data)

    def get_flow_config(self, flow_name: str) -> FlowConfig:
        """Get configuration for a specific flow."""
        if flow_name not in self.flows:
            raise ValueError(f"Flow '{flow_name}' not found in configuration")
        return self.flows[flow_name]
