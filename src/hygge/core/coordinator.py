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

from hygge.connections import (
    MSSQL_CONNECTION_DEFAULTS,
    ConnectionPool,
    MssqlConnection,
)
from hygge.utility.exceptions import ConfigError
from hygge.utility.logger import get_logger
from hygge.utility.path_helper import PathHelper
from hygge.utility.run_id import generate_run_id

from .flow import Flow, FlowConfig
from .home import Home
from .journal import Journal, JournalConfig
from .store import Store
from .workspace import Workspace, WorkspaceConfig

# Alias for backward compatibility - WorkspaceConfig is the canonical name
CoordinatorConfig = WorkspaceConfig

# Store-related configuration keys that can be applied as defaults
STORE_DEFAULT_KEYS = ["if_exists", "batch_size", "parallel_workers", "timeout"]


def validate_config(config: Dict[str, Any]) -> List[str]:
    """Validate configuration using Pydantic models."""
    try:
        WorkspaceConfig.from_dict(config)
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
        config: Optional[WorkspaceConfig] = None,
        flow_overrides: Optional[Dict[str, Any]] = None,
        flow_filter: Optional[List[str]] = None,
    ):
        """
        Initialize Coordinator.

        Args:
            config_path: Optional path to hygge.yml file (uses Workspace.from_path())
            config: Optional WorkspaceConfig instance (takes precedence)
            flow_overrides: Optional flow-level configuration overrides
            flow_filter: Optional list of flow names to execute
        """
        self.flows: List[Flow] = []
        self.options: Dict[str, Any] = {}
        self.connection_pools: Dict[str, ConnectionPool] = {}  # Named connection pools
        self.flow_overrides = flow_overrides or {}  # CLI overrides for flow configs
        self.flow_filter = flow_filter or []  # List of flow names to execute
        self.logger = get_logger("hygge.coordinator")
        # Workspace instance (for loading config)
        self._workspace: Optional[Workspace] = None

        # Flow results tracking for dbt-style summary
        self.flow_results: List[Dict[str, Any]] = []
        self.run_start_time: Optional[float] = None

        # If config provided directly, use it
        if config is not None:
            self.config = config
            self.config_path = None
            self.project_config = {}
            self.coordinator_name = "coordinator"
        elif config_path is None:
            # Project discovery mode - use Workspace to find hygge.yml
            workspace = Workspace.find()
            workspace._read_workspace_config()  # Populate workspace.config
            self.config = None  # Will be loaded in run() via workspace.prepare()
            self.config_path = workspace.hygge_yml
            self.project_config = workspace.config
            self.coordinator_name = workspace.name
            self._workspace = workspace
        else:
            # Explicit hygge.yml path provided
            self.config_path = Path(config_path)
            if self.config_path.name != "hygge.yml":
                raise ConfigError(
                    f"Expected hygge.yml, got {self.config_path.name}. "
                    "Only hygge.yml workspace configuration is supported."
                )
            workspace = Workspace.from_path(self.config_path)
            workspace._read_workspace_config()  # Populate workspace.config
            self.config = None  # Will be loaded in run() via workspace.prepare()
            self.project_config = workspace.config
            self.coordinator_name = workspace.name
            self._workspace = workspace

        # Extract options from config if available
        # Options are stored in project_config, not in CoordinatorConfig
        # So we need to get them from project_config
        if self.project_config:
            config_options = self.project_config.get("options", {})
            self.options = {**config_options, **self.options}

        # Extract journal config
        if self.config:
            self.journal_config = (
                self.config.journal
                if isinstance(self.config.journal, JournalConfig)
                else None
            )
        else:
            self.journal_config = None

        # No longer need Factory - using registry pattern directly
        self.coordinator_run_id: Optional[str] = None
        self.coordinator_start_time: Optional[datetime] = None
        self.journal: Optional[Journal] = None
        self._journal_cache: Dict[str, Journal] = {}
        self.flow_run_ids: Dict[str, str] = {}

    async def run(self) -> None:
        """Run all configured flows."""
        config_source = (
            str(self.config_path) if self.config_path else "Workspace-provided config"
        )
        self.logger.info(f"Starting coordinator with config: {config_source}")
        self.coordinator_start_time = datetime.now(timezone.utc)
        coordinator_start_iso = self.coordinator_start_time.isoformat()
        self.coordinator_run_id = generate_run_id(
            [self.coordinator_name, coordinator_start_iso]
        )
        self.flow_run_ids = {}

        try:
            # Load configuration if not already loaded
            if self.config is None:
                if not self._workspace:
                    raise ConfigError(
                        "No workspace available. Coordinator requires hygge.yml "
                        "workspace configuration."
                    )
                self.config = self._workspace.prepare()
                # Update project_config from workspace (in case it changed)
                self.project_config = self._workspace.config
                # Update journal_config from prepared config
                if self.config and self.config.journal:
                    if isinstance(self.config.journal, JournalConfig):
                        self.journal_config = self.config.journal
                    else:
                        self.journal_config = JournalConfig(**self.config.journal)

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
