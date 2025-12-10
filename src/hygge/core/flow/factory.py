"""
Flow factory for creating Flow instances with comfort and reliability.

The FlowFactory makes it easy to create Flow instances from configuration,
handling all the complexity of entity merging, validation, and wiring so
you can focus on your data flows.

Following hygge's philosophy, the factory prioritizes:
- **Comfort**: Simple creation from configuration, smart defaults
- **Reliability**: Proper validation, connection pool injection, journal wiring
- **Natural flow**: Handles entity merging and config resolution automatically
"""
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from hygge.homes.mssql import MssqlHome
from hygge.messages import get_logger
from hygge.utility.exceptions import ConfigError

from ..home import Home
from ..journal import Journal, JournalConfig
from ..store import Store

if TYPE_CHECKING:
    from .config import FlowConfig
    from .entity import Entity
    from .flow import Flow

# Store-related configuration keys that can be applied as defaults
STORE_DEFAULT_KEYS = ["if_exists", "batch_size", "parallel_workers", "timeout"]


class FlowFactory:
    """
    Factory for creating Flow instances with comfort and reliability.

    The FlowFactory makes it easy to create Flow instances from configuration,
    handling all the complexity so your flows are ready to run. It merges entity
    configs with flow configs, creates Home and Store instances, injects connection
    pools, wires up journals, and validates everything.

    Following hygge's philosophy, the factory prioritizes:
    - **Comfort**: Simple creation from configuration, handles complexity for you
    - **Reliability**: Proper validation, connection pool injection, journal wiring
    - **Natural flow**: Handles entity merging and config resolution automatically

    You typically don't call FlowFactory directly - the Coordinator uses it to
    create flows from your workspace configuration. But it's available if you
    need to create flows programmatically.
    """

    @staticmethod
    def from_config(
        flow_name: str,
        flow_config: "FlowConfig",  # type: ignore
        coordinator_run_id: str,
        coordinator_name: str,
        connection_pools: Dict[str, Any],
        journal_cache: Dict[str, Journal],
        flow_overrides: Optional[Dict[str, Any]] = None,
        get_or_create_journal: Optional[Callable] = None,
        logger: Optional[Any] = None,
        flow_class: Optional[type] = None,
    ) -> "Flow":  # type: ignore
        """
        Create a Flow instance from its configuration.

        Creates Home and Store instances, wires up journals and connection pools,
        and makes everything ready to run.

        Args:
            flow_name: Name of the flow
            flow_config: Flow configuration
            coordinator_run_id: Coordinator run ID
            coordinator_name: Coordinator name
            connection_pools: Available connection pools
            journal_cache: Journal instance cache
            flow_overrides: Optional CLI overrides for flow config
            get_or_create_journal: Function to get or create journal instances
            logger: Logger instance
            flow_class: Flow class to instantiate (for dependency injection in tests)

        Returns:
            Flow instance ready to run
        """
        from .flow import Flow  # Avoid circular import

        FlowCls = flow_class or Flow
        log = logger or get_logger(f"hygge.flow.{flow_name}")

        # Apply overrides if provided
        if flow_overrides:
            flow_config = FlowFactory._apply_overrides(
                flow_config, flow_name, flow_overrides
            )

        # Determine run type
        default_run_type = FlowFactory._extract_run_type(flow_config)

        # Get home and store configs
        home_config = flow_config.get_home_config()
        store_config = flow_config.get_store_config()

        # Validate configs are accessible
        if not home_config:
            raise ConfigError(
                f"Cannot create flow '{flow_name}': "
                f"home config is missing or invalid. "
                f"Expected dict with 'type' field, got {type(flow_config.home)}"
            )
        if not store_config:
            raise ConfigError(
                f"Cannot create flow '{flow_name}': "
                f"store config is missing or invalid. "
                f"Expected dict with 'type' field, got {type(flow_config.store)}"
            )

        # For Open Mirroring stores without entities,
        # key_columns must be provided at flow level
        if store_config.type == "open_mirroring":
            if (
                not hasattr(store_config, "key_columns")
                or store_config.key_columns is None
                or len(store_config.key_columns) == 0
            ):
                raise ConfigError(
                    f"Flow '{flow_name}' uses Open Mirroring store but "
                    f"does not have entities. key_columns must be provided "
                    f"at the flow level in store config."
                )

        # Create home and store instances
        home = FlowFactory._create_home_instance(
            flow_name, home_config, connection_pools, None, log
        )
        store = FlowFactory._create_store_instance(
            flow_name, store_config, flow_name, None, connection_pools, log
        )

        # Validate run_type and store incremental alignment
        FlowFactory._validate_run_type_alignment(
            store_config, default_run_type, flow_name, None, log
        )

        # Get or create journal instance
        flow_journal_config = (
            flow_config.journal
            if isinstance(flow_config.journal, JournalConfig)
            else None
        )
        journal_instance = FlowFactory._get_or_create_journal(
            flow_journal_config,
            store_config,
            home_config,
            store,
            get_or_create_journal,
        )

        # Build flow options
        flow_options = FlowFactory._build_flow_options(flow_config)

        return FlowCls(
            flow_name,
            home,
            store,
            flow_options,
            journal=journal_instance,
            coordinator_run_id=coordinator_run_id,
            flow_run_id=None,
            coordinator_name=coordinator_name,
            base_flow_name=flow_name,
            entity_name=flow_name,  # Always set for non-entity flows
            run_type=default_run_type,
            watermark_config=flow_config.watermark,
        )

    @staticmethod
    def from_entity(
        entity: "Entity",  # type: ignore
        coordinator_run_id: str,
        coordinator_name: str,
        connection_pools: Dict[str, Any],
        journal_cache: Dict[str, Journal],
        get_or_create_journal: Optional[Callable] = None,
        logger: Optional[Any] = None,
        flow_class: Optional[type] = None,
    ) -> "Flow":  # type: ignore
        """
        Create a Flow instance from an Entity (already merged and validated).

        Entity contains the fully merged FlowConfig (entity overrides applied),
        so no config merging is needed. This method just creates Home/Store
        instances and wires up the Flow.

        Args:
            entity: Entity with merged configuration (from Workspace)
            coordinator_run_id: Coordinator run ID
            coordinator_name: Coordinator name
            connection_pools: Available connection pools
            journal_cache: Journal instance cache
            get_or_create_journal: Function to get or create journal instances
            logger: Logger instance
            flow_class: Flow class to instantiate (for dependency injection in tests)

        Returns:
            Flow instance ready to run
        """
        from .flow import Flow  # Avoid circular import

        FlowCls = flow_class or Flow
        log = logger or get_logger(f"hygge.flow.{entity.flow_name}")

        # Entity already has merged config, so use it directly
        flow_config = entity.flow_config

        # Get home and store configs from merged flow config
        home_config = flow_config.get_home_config()
        store_config = flow_config.get_store_config()

        # Validate configs are accessible with specific error messages
        if not home_config:
            raise ConfigError(
                f"Cannot create flow from entity '{entity.flow_name}': "
                f"home config is missing or invalid. "
                f"Expected dict with 'type' field, got {type(flow_config.home)}. "
                f"Check that home configuration in flow "
                f"'{entity.base_flow_name}' is properly defined."
            )  # noqa: E501
        if not store_config:
            raise ConfigError(
                f"Cannot create flow from entity '{entity.flow_name}': "
                f"store config is missing or invalid. "
                f"Expected dict with 'type' field, got {type(flow_config.store)}. "
                f"Check that store configuration in flow "
                f"'{entity.base_flow_name}' is properly defined."
            )  # noqa: E501

        # Extract run_type and watermark from merged flow config
        entity_run_type = FlowFactory._extract_run_type(flow_config)
        entity_watermark = flow_config.watermark

        # Get journal config from merged flow config
        entity_journal_config = (
            flow_config.journal
            if isinstance(flow_config.journal, JournalConfig)
            else None
        )

        # Create home and store instances
        # For entity flows, entity_name is passed to Home/Store for path handling
        home = FlowFactory._create_home_instance(
            entity.flow_name,
            home_config,
            connection_pools,
            entity.entity_name,
            log,
            entity.entity_config,
        )
        store = FlowFactory._create_store_instance(
            entity.flow_name,
            store_config,
            entity.flow_name,
            entity.entity_name,
            connection_pools,
            log,
        )

        # Validate run_type and store incremental alignment
        FlowFactory._validate_run_type_alignment(
            store_config,
            entity_run_type,
            entity.base_flow_name,
            entity.entity_name,
            log,
        )

        # Get or create journal instance
        journal_instance = FlowFactory._get_or_create_journal(
            entity_journal_config,
            store_config,
            home_config,
            store,
            get_or_create_journal,
        )

        # Build flow options
        flow_options = FlowFactory._build_flow_options(flow_config)

        # Ensure entity_name is always set
        # (use flow_name as fallback for non-entity flows)
        entity_name = entity.entity_name or entity.flow_name

        return FlowCls(
            entity.flow_name,
            home,
            store,
            flow_options,
            journal=journal_instance,
            coordinator_run_id=coordinator_run_id,
            flow_run_id=None,
            coordinator_name=coordinator_name,
            base_flow_name=entity.base_flow_name,
            entity_name=entity_name,  # Always set (never None)
            run_type=entity_run_type,
            watermark_config=entity_watermark,
        )

    @staticmethod
    def _extract_run_type(flow_config: "FlowConfig") -> str:  # type: ignore
        """
        Extract run type from flow config.

        Args:
            flow_config: Flow configuration

        Returns:
            Run type string: "full_drop" or "incremental"
        """
        run_type = flow_config.run_type or "full_drop"
        if flow_config.full_drop is not None:
            run_type = "full_drop" if flow_config.full_drop else "incremental"
        return run_type

    @staticmethod
    def _create_home_instance(
        flow_name: str,
        home_config: Any,
        connection_pools: Dict[str, Any],
        entity_name: Optional[str],
        logger: Any,
        entity_config: Optional[Dict[str, Any]] = None,
    ) -> Home:
        """
        Create home instance with connection pool injection.

        Args:
            flow_name: Name of the flow
            home_config: Home configuration
            connection_pools: Available connection pools
            entity_name: Optional entity name for path handling
            logger: Logger instance
            entity_config: Optional entity config for path handling logic

        Returns:
            Home instance
        """
        # Get connection pool for home if needed (MSSQL homes only)
        pool: Optional[Any] = None
        if home_config.type == "mssql":
            if hasattr(home_config, "connection") and home_config.connection:
                pool = connection_pools.get(home_config.connection)
                if pool is None:
                    conn_name = home_config.connection
                    logger.warning(f"Connection '{conn_name}' referenced but not found")

        # Create home instance
        if home_config.type == "mssql":
            return MssqlHome(
                f"{flow_name}_home", home_config, pool=pool, entity_name=entity_name
            )
        else:
            # For parquet homes, check if entity config specified path
            # (prevents double path appending)
            if (
                entity_config
                and isinstance(entity_config, dict)
                and "home" in entity_config
                and "path" in entity_config["home"]
            ):
                return Home.create(f"{flow_name}_home", home_config)
            else:
                return Home.create(f"{flow_name}_home", home_config, entity_name)

    @staticmethod
    def _create_store_instance(
        flow_name: str,
        store_config: Any,
        store_flow_name: str,
        entity_name: Optional[str],
        connection_pools: Dict[str, Any],
        logger: Any,
    ) -> Store:
        """
        Create store instance with connection pool injection.

        Args:
            flow_name: Name of the flow (for store name)
            store_config: Store configuration
            store_flow_name: Flow name to pass to Store.create
            entity_name: Optional entity name for path handling
            connection_pools: Available connection pools
            logger: Logger instance

        Returns:
            Store instance
        """
        store = Store.create(
            f"{flow_name}_store", store_config, store_flow_name, entity_name
        )

        # Inject connection pool into stores that need it
        FlowFactory._inject_connection_pool(
            store, store_config, connection_pools, logger
        )

        return store

    @staticmethod
    def _get_or_create_journal(
        journal_config: Optional[JournalConfig],
        store_config: Any,
        home_config: Any,
        store: Store,
        get_or_create_journal: Optional[Callable],
    ) -> Optional[Journal]:
        """
        Get or create journal instance.

        Args:
            journal_config: Journal configuration
            store_config: Store configuration
            home_config: Home configuration
            store: Store instance
            get_or_create_journal: Function to get or create journal instances

        Returns:
            Journal instance or None
        """
        if not get_or_create_journal:
            return None
        return get_or_create_journal(journal_config, store_config, home_config, store)

    @staticmethod
    def _build_flow_options(flow_config: "FlowConfig") -> Dict[str, Any]:  # type: ignore
        """
        Build flow options from flow config.

        Args:
            flow_config: Flow configuration

        Returns:
            Flow options dictionary
        """
        flow_options = flow_config.options.copy()
        flow_options.update(
            {
                "queue_size": flow_config.queue_size,
                "timeout": flow_config.timeout,
            }
        )
        return flow_options

    @staticmethod
    def _apply_overrides(
        flow_config: "FlowConfig",  # type: ignore
        flow_name: str,
        flow_overrides: Dict[str, Any],
    ) -> "FlowConfig":  # type: ignore
        """
        Apply CLI flow-level overrides to flow configuration.

        Args:
            flow_config: FlowConfig to override
            flow_name: Name of the flow
            flow_overrides: Dict mapping flow names to override dicts

        Returns:
            Updated FlowConfig with overrides applied
        """
        from .config import FlowConfig  # Avoid circular import

        if not flow_overrides or flow_name not in flow_overrides:
            return flow_config

        overrides = flow_overrides[flow_name]
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

    @staticmethod
    def _validate_run_type_alignment(
        store_config: Any,
        run_type: str,
        flow_name: str,
        entity_name: Optional[str],
        logger: Any,
    ) -> None:
        """Validate run_type and store incremental alignment."""
        # Only validate ADLS-family stores (ADLS, OneLake, OpenMirroring)
        # They have the incremental field
        if not hasattr(store_config, "incremental"):
            return

        incremental_override = getattr(store_config, "incremental", None)
        if incremental_override is None:
            # No override set, so store defers to flow run_type - this is fine
            return

        context = f"flow '{flow_name}'"
        if entity_name:
            context = f"entity '{entity_name}' in {context}"

        # Determine expected behavior from run_type
        is_incremental_from_run_type = run_type != "full_drop"

        # Check if store override matches flow run_type
        if incremental_override != is_incremental_from_run_type:
            if incremental_override:
                # Store forces incremental, but flow is full_drop
                logger.warning(
                    f"Flow {context} has run_type='{run_type}' (full reload), "
                    f"but store has incremental=True (append mode). "
                    f"Store will append new data instead of truncating. "
                    f"Consider setting store.incremental=False or "
                    f"flow run_type='incremental'."
                )
            else:
                # Store forces full_drop, but flow is incremental
                logger.warning(
                    f"Flow {context} has run_type='{run_type}' (incremental), "
                    f"but store has incremental=False (truncate mode). "
                    f"Store will truncate destination before writing new data. "
                    f"Consider setting store.incremental=True or "
                    f"flow run_type='full_drop'."
                )

    @staticmethod
    def _inject_connection_pool(
        store: Store,
        store_config: Any,
        connection_pools: Dict[str, Any],
        logger: Any,
    ) -> None:
        """Inject connection pool into stores that need it."""
        # Default implementation is no-op, so always safe to call
        if not hasattr(store_config, "connection"):
            return

        if not store_config.connection:
            return

        pool = connection_pools.get(store_config.connection)
        if pool:
            store.set_pool(pool)
        else:
            conn_name = store_config.connection
            logger.warning(f"Connection '{conn_name}' referenced but not found")
