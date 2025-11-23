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
from typing import Any, Dict, List, Optional

from hygge.connections import (
    MSSQL_CONNECTION_DEFAULTS,
    ConnectionPool,
    MssqlConnection,
)
from hygge.messages import Progress, Summary, get_logger
from hygge.utility.exceptions import ConfigError
from hygge.utility.run_id import generate_run_id

from .flow import Flow, FlowConfig, FlowFactory
from .journal import Journal, JournalConfig
from .workspace import Workspace, WorkspaceConfig

# Alias for backward compatibility - WorkspaceConfig is the canonical name
CoordinatorConfig = WorkspaceConfig


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

        # Flow results tracking for hygge-style summary
        self.flow_results: List[Dict[str, Any]] = []
        self.run_start_time: Optional[float] = None

        # Initialize progress and summary
        self.progress = Progress(logger=self.logger)
        self.summary = Summary(logger=self.logger)

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

                # Determine default run type and watermark
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
                            entity_config = entity
                        # Handle dict entities (project-centric pattern)
                        elif isinstance(entity, dict):
                            entity_name = entity.get("name")
                            if not entity_name:
                                raise ConfigError(
                                    f"Entity in flow {base_flow_name} "
                                    f"missing 'name' field"
                                )
                            entity_config = entity
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

                        # Create entity flow using Flow.from_entity()
                        flow = Flow.from_entity(
                            flow_name=entity_flow_name,
                            base_flow_name=base_flow_name,
                            flow_config=flow_config,
                            entity_name=entity_name,
                            entity_config=entity_config,
                            coordinator_run_id=self.coordinator_run_id,
                            coordinator_name=self.coordinator_name,
                            connection_pools=self.connection_pools,
                            journal_cache=self._journal_cache,
                            flow_journal_config=flow_journal_config,
                            default_run_type=default_run_type,
                            default_watermark=default_watermark,
                            get_or_create_journal=self._get_or_create_journal_instance,
                            logger=self.logger,
                        )
                        self.flows.append(flow)
                else:
                    # Create single flow without entities using Flow.from_config()
                    # Apply CLI flow-level overrides and update config in place
                    # (for test verification)
                    if self.flow_overrides:
                        flow_config = FlowFactory._apply_overrides(
                            flow_config, base_flow_name, self.flow_overrides
                        )
                        # Update config in place so it's accessible later
                        self.config.flows[base_flow_name] = flow_config

                    # Create non-entity flow using Flow.from_config()
                    # Pass None for flow_overrides since we already applied them above
                    flow = Flow.from_config(
                        flow_name=base_flow_name,
                        flow_config=flow_config,
                        coordinator_run_id=self.coordinator_run_id,
                        coordinator_name=self.coordinator_name,
                        connection_pools=self.connection_pools,
                        journal_cache=self._journal_cache,
                        flow_overrides=None,  # Already applied above
                        get_or_create_journal=self._get_or_create_journal_instance,
                        logger=self.logger,
                    )
                    self.flows.append(flow)

                self.logger.debug(f"Created flow: {base_flow_name}")

            except Exception as e:
                # Preserve entity context in error message if available
                error_msg = str(e)
                # entity_name might not be defined if error occurs before entity loop
                if "entity" not in error_msg.lower():
                    # Try to extract entity name from error or flow name
                    if "_" in base_flow_name and "entity" not in error_msg.lower():
                        # Flow name might be base_entity, try to extract
                        parts = base_flow_name.split("_", 1)
                        if len(parts) > 1:
                            potential_entity = parts[-1]
                            error_msg = f"Entity '{potential_entity}': {error_msg}"
                raise ConfigError(
                    f"Failed to create flow {base_flow_name}: {error_msg}"
                )

    async def _run_flows(self) -> None:
        """Run all flows in parallel with dbt-style logging and concurrency limiting."""
        if not self.flows:
            self.logger.warning("No flows to run")
            return

        # Reset flow results for this run
        self.flow_results = []
        self.run_start_time = asyncio.get_event_loop().time()

        # Start progress tracking
        self.progress.start(self.run_start_time)

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
            # Progress.update() handles milestone tracking
            async def progress_callback(rows):
                await self.progress.update(rows)

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

        # Generate and log hygge-style summary BEFORE checking for failures
        # This ensures the summary is always shown, even when flows fail
        self.summary.generate_summary(self.flow_results, self.run_start_time)

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
