"""
Flow configuration model for comfortable, convention-over-configuration setup.

FlowConfig defines the structure for flow configurations, following hygge's
philosophy of convention over configuration. Simple configs "just work" with
smart defaults, while advanced configs give you full control when needed.

FlowConfig includes home/store definitions, entity configurations, and flow-level
settings, all designed to feel natural and comfortable to use.
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator

from ..home import Home, HomeConfig
from ..journal import JournalConfig
from ..store import Store, StoreConfig

# Default configuration type for string-based home/store paths
DEFAULT_CONFIG_TYPE = "parquet"


class FlowConfig(BaseModel):
    """
    Configuration for a data flow - simple by default, powerful when needed.

    FlowConfig follows hygge's philosophy of convention over configuration.
    Simple configs "just work" with smart defaults, while advanced configs
    give you full control when needed.

    Flow configurations are defined in `flows/<flow_name>/flow.yml` files
    as part of the workspace pattern. Each flow defines its home (source)
    and store (destination).

    Simple (convention over configuration - just works):
    ```yaml
    # flows/users_to_lake/flow.yml
    name: users_to_lake
    home:
      type: parquet
      path: data/users.parquet
    store:
      type: parquet
      path: data/lake/users
    ```

    Advanced (full control when you need it):
    ```yaml
    # flows/users_to_lake/flow.yml
    name: users_to_lake
    home:
      type: mssql
      connection: my_database
      table: dbo.users
    store:
      type: parquet
      path: data/lake/users
      options:
        compression: snappy
    ```

    See the workspace documentation for project structure details.
    """

    # Clean, simple configuration - only home/store
    home: Union[str, Dict[str, Any]] = Field(..., description="Home configuration")
    store: Union[str, Dict[str, Any]] = Field(..., description="Store configuration")
    queue_size: int = Field(
        default=10, ge=1, le=100, description="Size of internal queue"
    )
    timeout: int = Field(default=300, ge=1, description="Operation timeout in seconds")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional flow options"
    )
    entities: Optional[Union[List[str], List[Dict[str, Any]]]] = Field(
        default=None, description="Entity names or definitions for this flow"
    )
    # Flow-level strategy: full_drop for full reloads
    full_drop: Optional[bool] = Field(
        default=None,
        description=(
            "Compatibility flag for legacy configs. When set, overrides "
            "the default run_type: true → run_type 'full_drop', "
            "false → run_type 'incremental'."
        ),
    )
    # Journal configuration (optional)
    journal: Optional[Union[Dict[str, Any], JournalConfig]] = Field(
        default=None,
        description="Journal configuration for tracking flow execution metadata",
    )
    # Watermark configuration (flow-level, applies to all entities unless overridden)
    watermark: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Watermark configuration for incremental loads. "
            "Applies to all entities unless overridden at entity level. "
            "Requires 'primary_key' and 'watermark_column'."
        ),
    )
    # Run type (flow-level default, can be overridden at entity level)
    run_type: Optional[str] = Field(
        default="full_drop",
        description=(
            "Run type for this flow: 'full_drop' (default) or 'incremental'. "
            "Can be overridden at entity level."
        ),
    )

    @field_validator("home", mode="before")
    @classmethod
    def validate_home(cls, v):
        """
        Validate home configuration structure (lenient - structure only).

        FlowConfig is a template that may have incomplete configs waiting for
        entity overrides. This validator checks structure (type exists, valid type)
        but allows incomplete configs to pass through. Full validation happens
        when Entity is created (after entity merging).
        """
        if isinstance(v, str):
            # For strings, validate by trying to create HomeConfig
            try:
                HomeConfig.create(v)
            except Exception as e:
                # Re-raise the original ValidationError
                raise e
        elif isinstance(v, dict):
            if not v:
                raise ValueError("Home configuration cannot be empty")
            # Lenient validation: only check structure (type exists and is valid)
            # Allow incomplete configs to pass through
            # (entity configs will complete them)
            config_type = v.get("type", "parquet")  # Default to parquet
            if config_type not in HomeConfig._registry:
                raise ValueError(f"Unknown home config type: {config_type}")
            # Don't validate completeness - that happens when FlowInstance is created
        return v

    @field_validator("store", mode="before")
    @classmethod
    def validate_store(cls, v):
        """
        Validate store configuration structure (lenient - structure only).

        FlowConfig is a template that may have incomplete configs waiting for
        entity overrides. This validator checks structure (type exists, valid type)
        but allows incomplete configs to pass through. Full validation happens
        when Entity is created (after entity merging).

        This allows flow-level store configs to be incomplete (e.g., missing
        key_columns for Open Mirroring) if entities will provide them via
        entity.store.key_columns.
        """
        if isinstance(v, str):
            # For strings, validate by trying to create StoreConfig
            try:
                StoreConfig.create(v)
            except Exception as e:
                # Re-raise the original ValidationError
                raise e
        elif isinstance(v, dict):
            if not v:
                raise ValueError("Store configuration cannot be empty")
            # Lenient validation: only check structure (type exists and is valid)
            # Allow incomplete configs to pass through
            # (entity configs will complete them)
            config_type = v.get("type", "parquet")  # Default to parquet
            if config_type not in StoreConfig._registry:
                raise ValueError(f"Unknown store config type: {config_type}")
            # Don't validate completeness - that happens when FlowInstance is created
        return v

    @field_validator("journal", mode="before")
    @classmethod
    def validate_journal(cls, v):
        """Ensure journal configuration is parsed into JournalConfig."""
        if v is None:
            return None
        if isinstance(v, JournalConfig):
            return v
        if isinstance(v, dict):
            return JournalConfig(**v)
        raise ValueError(
            "Journal configuration must be a dict or JournalConfig instance"
        )

    @property
    def home_instance(self) -> Home:
        """Get home instance - converts raw config to Home instance."""
        if isinstance(self.home, str):
            # Simple string configuration
            config = HomeConfig.create(self.home)
            return Home.create("flow_home", config)
        elif isinstance(self.home, dict):
            # Dictionary configuration - create Home instance
            config = HomeConfig.create(self.home)
            return Home.create("flow_home", config)
        else:
            # Already a Home instance
            return self.home

    @property
    def store_instance(self) -> Store:
        """Get store instance - converts raw config to Store instance."""
        if isinstance(self.store, str):
            # Simple string configuration
            config = StoreConfig.create(self.store)
            return Store.create("", config)
        elif isinstance(self.store, dict):
            # Dictionary configuration - create Store instance
            config = StoreConfig.create(self.store)
            return Store.create("", config)
        else:
            # Already a Store instance
            return self.store

    @property
    def home_config(self) -> HomeConfig:
        """Get home config - converts raw config to HomeConfig."""
        home_instance = self.home_instance
        return home_instance.config

    @property
    def store_config(self) -> StoreConfig:
        """Get store config - converts raw config to StoreConfig."""
        store_instance = self.store_instance
        return store_instance.config

    def get_store_config(self) -> StoreConfig:
        """
        Get store config without creating store instance.

        Returns StoreConfig from raw store data without triggering
        store creation or validation. Safe to call before entity
        configs are merged.

        Returns:
            StoreConfig instance
        """
        if isinstance(self.store, dict):
            return StoreConfig.create(self.store)
        elif isinstance(self.store, str):
            return StoreConfig.create({"type": DEFAULT_CONFIG_TYPE, "path": self.store})
        elif hasattr(self.store, "model_dump"):
            # Already a StoreConfig
            return self.store
        elif hasattr(self.store, "config"):
            # Already a Store instance
            return self.store.config
        else:
            # Fallback to property (may trigger validation)
            return self.store_config

    def get_home_config(self) -> HomeConfig:
        """
        Get home config without creating home instance.

        Returns HomeConfig from raw home data without triggering
        home creation. Safe to call before entity configs are merged.

        Returns:
            HomeConfig instance
        """
        if isinstance(self.home, dict):
            return HomeConfig.create(self.home)
        elif isinstance(self.home, str):
            return HomeConfig.create({"type": DEFAULT_CONFIG_TYPE, "path": self.home})
        elif hasattr(self.home, "model_dump"):
            # Already a HomeConfig
            return self.home
        elif hasattr(self.home, "config"):
            # Already a Home instance
            return self.home.config
        else:
            # Fallback to property (may trigger validation)
            return self.home_config
