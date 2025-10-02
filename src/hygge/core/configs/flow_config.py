"""
Configuration for data flows.
"""
from typing import Any, Dict, Union

from pydantic import BaseModel, Field, field_validator

from .home_config import HomeConfig
from .settings import settings
from .store_config import StoreConfig


class FlowDefaults(BaseModel):
    """Default options for flows."""
    queue_size: int = Field(
        default=settings.flow_queue_size,
        ge=1, le=100,
        description="Size of internal queue"
    )
    timeout: int = Field(
        default=settings.flow_timeout,
        ge=1,
        description="Operation timeout in seconds"
    )


class FlowConfig(BaseModel):
    """
    Configuration for a data flow.

    Supports both simple and advanced configurations:

    Simple (Rails spirit - convention over configuration):
    ```yaml
    flows:
      users_to_lake:
        home: data/users.parquet
        store: data/lake/users
    ```

    Advanced (full control):
    ```yaml
    flows:
      users_to_lake:
        home:
          type: sql
          table: users
          connection: ${DATABASE_URL}
        store:
          type: parquet
          path: data/lake/users
          options:
            compression: snappy
    ```
    """
    # Clean, simple configuration - only home/store, no legacy from/to
    home: Union[str, HomeConfig] = Field(..., description="Home configuration")
    store: Union[str, StoreConfig] = Field(..., description="Store configuration")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Flow options"
    )

    @field_validator('home', mode='before')
    @classmethod
    def parse_home(cls, v):
        """Parse home configuration from string or dict with smart defaults."""
        if isinstance(v, str):
            # Simple path - detect type from extension or use default type
            if v.endswith('.parquet'):
                home_type = 'parquet'
            else:
                home_type = settings.get_home_type()

            return HomeConfig(
                type=home_type,
                path=v,
                options=settings.get_home_settings()
            )
        elif isinstance(v, dict):
            # Advanced configuration - apply smart defaults to options
            # If type not specified, use default
            if 'type' not in v:
                v['type'] = settings.get_home_type()
            options = v.get('options', {})
            v['options'] = settings.apply_home_settings(options)
            return HomeConfig(**v)
        return v

    @field_validator('store', mode='before')
    @classmethod
    def parse_store(cls, v):
        """Parse store configuration from string or dict with smart defaults."""
        if isinstance(v, str):
            # Simple path - use default type with smart defaults
            return StoreConfig(
                type=settings.get_store_type(),
                path=v,
                options=settings.get_store_settings()
            )
        elif isinstance(v, dict):
            # Advanced configuration - apply smart defaults to options
            # If type not specified, use default
            if 'type' not in v:
                v['type'] = settings.get_store_type()
            options = v.get('options', {})
            v['options'] = settings.apply_store_settings(options)
            return StoreConfig(**v)
        return v

    @property
    def home_config(self) -> HomeConfig:
        """Get home configuration - always returns HomeConfig after validation."""
        return self.home

    @property
    def store_config(self) -> StoreConfig:
        """Get store configuration - always returns StoreConfig after validation."""
        return self.store
