"""
Flow factory for creating Flow instances from configuration.

The FlowFactory provides methods to construct Flow instances from
configuration, handling entity merging, validation, and wiring.
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
    Factory for creating Flow instances from configuration.

    Handles the construction of Flow instances, including:
    - Config merging (entity configs with flow configs)
    - Home and Store instance creation
    - Connection pool injection
    - Journal wiring
    - Validation
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
        default_run_type = flow_config.run_type or "full_drop"
        if flow_config.full_drop is not None:
            default_run_type = "full_drop" if flow_config.full_drop else "incremental"

        # Get home config and create home instance
        home_config = flow_config.get_home_config()

        # Get connection pool for home if needed (MSSQL homes only)
        pool: Optional[Any] = None
        if home_config.type == "mssql":
            if hasattr(home_config, "connection") and home_config.connection:
                pool = connection_pools.get(home_config.connection)
                if pool is None:
                    conn_name = home_config.connection
                    log.warning(f"Connection '{conn_name}' referenced but not found")

        # Create home instance
        if home_config.type == "mssql":
            home = MssqlHome(f"{flow_name}_home", home_config, pool=pool)
        else:
            home = Home.create(f"{flow_name}_home", home_config)

        # Get store config and create store instance
        store_config = flow_config.get_store_config()

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

        store = Store.create(f"{flow_name}_store", store_config, flow_name, None)

        # Validate run_type and store incremental alignment
        FlowFactory._validate_run_type_alignment(
            store_config, default_run_type, flow_name, None, log
        )

        # Inject connection pool into stores that need it
        FlowFactory._inject_connection_pool(store, store_config, connection_pools, log)

        # Get or create journal instance
        journal_instance = None
        flow_journal_config = (
            flow_config.journal
            if isinstance(flow_config.journal, JournalConfig)
            else None
        )
        if get_or_create_journal:
            journal_instance = get_or_create_journal(
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
            entity_name=None,
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

        if not home_config or not store_config:
            raise ConfigError(
                f"Cannot create flow from entity '{entity.flow_name}': "
                "home/store configs not accessible"
            )

        # Extract run_type and watermark from merged flow config
        entity_run_type = flow_config.run_type or "full_drop"
        if flow_config.full_drop is not None:
            entity_run_type = "full_drop" if flow_config.full_drop else "incremental"
        entity_watermark = flow_config.watermark

        # Get journal config from merged flow config
        entity_journal_config = (
            flow_config.journal
            if isinstance(flow_config.journal, JournalConfig)
            else None
        )

        # Get connection pool for home if needed (MSSQL homes only)
        pool: Optional[Any] = None
        if home_config.type == "mssql":
            if hasattr(home_config, "connection") and home_config.connection:
                pool = connection_pools.get(home_config.connection)
                if pool is None:
                    conn_name = home_config.connection
                    log.warning(f"Connection '{conn_name}' referenced but not found")

        # Create home and store instances
        # For entity flows, entity_name is passed to Home/Store for path handling
        if home_config.type == "mssql":
            home = MssqlHome(
                f"{entity.flow_name}_home",
                home_config,
                pool=pool,
                entity_name=entity.entity_name,
            )
        else:
            # For parquet homes, check if entity config specified path
            # (prevents double path appending)
            if (
                entity.entity_config
                and isinstance(entity.entity_config, dict)
                and "home" in entity.entity_config
                and "path" in entity.entity_config["home"]
            ):
                home = Home.create(f"{entity.flow_name}_home", home_config)
            else:
                home = Home.create(
                    f"{entity.flow_name}_home", home_config, entity.entity_name
                )

        store = Store.create(
            f"{entity.flow_name}_store",
            store_config,
            entity.flow_name,
            entity.entity_name,
        )

        # Validate run_type and store incremental alignment
        FlowFactory._validate_run_type_alignment(
            store_config,
            entity_run_type,
            entity.base_flow_name,
            entity.entity_name,
            log,
        )

        # Inject connection pool into stores that need it
        FlowFactory._inject_connection_pool(store, store_config, connection_pools, log)

        # Get or create journal instance
        journal_instance = None
        if get_or_create_journal:
            journal_instance = get_or_create_journal(
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
            entity_name=entity.entity_name,
            run_type=entity_run_type,
            watermark_config=entity_watermark,
        )

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
        if not hasattr(store, "set_pool"):
            return

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
