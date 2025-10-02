"""
Configuration for data flows.
"""
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field, field_validator

from .home_config import HomeConfig
from .store_config import StoreConfig


class FlowDefaults(BaseModel):
    """Default options for flows."""
    queue_size: int = Field(
        default=10, ge=1, le=100, description="Size of internal queue"
    )
    timeout: int = Field(
        default=300, ge=1, description="Operation timeout in seconds"
    )


class FlowConfig(BaseModel):
    """Configuration for a data flow."""
    # Minimal configuration (Rails spirit!)
    from_path: Optional[str] = Field(None, alias='from', description="Source path")
    to_path: Optional[str] = Field(None, alias='to', description="Destination path")

    # Advanced configuration
    home: Optional[Union[str, HomeConfig]] = Field(
        None, description="Home configuration"
    )
    store: Optional[Union[str, StoreConfig]] = Field(
        None, description="Store configuration"
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Flow options"
    )

    @field_validator('home', mode='before')
    @classmethod
    def parse_home(cls, v):
        """Parse home configuration from string or dict."""
        if isinstance(v, str):
            return HomeConfig(type='parquet', path=v)
        elif isinstance(v, dict):
            return HomeConfig(**v)
        return v

    @field_validator('store', mode='before')
    @classmethod
    def parse_store(cls, v):
        """Parse store configuration from string or dict."""
        if isinstance(v, str):
            return StoreConfig(type='parquet', path=v)
        elif isinstance(v, dict):
            return StoreConfig(**v)
        return v

    def get_home_config(self, flow_name: str) -> HomeConfig:
        """Get home configuration with defaults applied."""
        if self.from_path:
            return HomeConfig(type='parquet', path=self.from_path)
        elif self.home:
            if isinstance(self.home, str):
                return HomeConfig(type='parquet', path=self.home)
            return self.home
        else:
            raise ValueError(f"Flow '{flow_name}' missing home configuration")

    def get_store_config(self, flow_name: str) -> StoreConfig:
        """Get store configuration with defaults applied."""
        if self.to_path:
            return StoreConfig(type='parquet', path=self.to_path)
        elif self.store:
            if isinstance(self.store, str):
                return StoreConfig(type='parquet', path=self.store)
            return self.store
        else:
            raise ValueError(f"Flow '{flow_name}' missing store configuration")
