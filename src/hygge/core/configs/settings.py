"""
Centralized configuration settings for hygge.

This module provides configurable settings that embody hygge's philosophy:
- Comfort: Sensible values that work well in most cases
- Simplicity: Easy to understand and customize
- Reliability: Tested values that provide good performance
- Flow: Smooth data movement with appropriate batch sizes
"""
from typing import Any, Dict

from pydantic import BaseModel, Field, field_validator


class HyggeSettings(BaseModel):
    """
    Centralized settings for hygge components.

    These settings embody hygge's "Convention over Configuration" philosophy:
    - They work well for most use cases
    - They can be easily customized for your environment
    - They follow Rails-inspired smart settings
    """

    # Type settings - what to use when not specified
    home_type: str = Field(
        default='parquet',
        description="Most common data source format"
    )
    store_type: str = Field(
        default='parquet',
        description="Most common data destination format"
    )

    # Home settings - optimized for reading
    home_batch_size: int = Field(
        default=10000,
        ge=1,
        description="Comfortable reading batch size"
    )
    home_row_multiplier: int = Field(
        default=300000,
        ge=1,
        description="Memory-friendly multiplier"
    )

    # Store settings - optimized for writing
    store_batch_size: int = Field(
        default=100000,
        ge=1,
        description="Larger batches for efficient writing"
    )
    store_compression: str = Field(
        default='snappy',
        description="Good balance of speed/size"
    )

    # Flow settings - orchestration configuration
    flow_queue_size: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Reasonable concurrency"
    )
    flow_timeout: int = Field(
        default=300,
        ge=1,
        description="5 minutes timeout"
    )

    @field_validator('home_type', 'store_type')
    @classmethod
    def validate_type(cls, v):
        """Validate type settings."""
        valid_types = ['parquet', 'sql', 'csv']
        if v not in valid_types:
            raise ValueError(f"Type must be one of {valid_types}, got '{v}'")
        return v

    @field_validator('store_compression')
    @classmethod
    def validate_compression(cls, v):
        """Validate compression setting."""
        valid_compressions = ['snappy', 'gzip', 'zstd', 'lz4', 'brotli']
        if v not in valid_compressions:
            raise ValueError(
                f"Compression must be one of {valid_compressions}, got '{v}'"
            )
        return v

    def get_home_type(self) -> str:
        """Get configured home type setting."""
        return self.home_type

    def get_store_type(self) -> str:
        """Get configured store type setting."""
        return self.store_type

    def get_home_settings(self) -> Dict[str, Any]:
        """Get configured options for home configurations."""
        return {
            'batch_size': self.home_batch_size,
            'row_multiplier': self.home_row_multiplier
        }

    def get_store_settings(self) -> Dict[str, Any]:
        """Get configured options for store configurations."""
        return {
            'batch_size': self.store_batch_size,
            'compression': self.store_compression
        }

    def get_flow_settings(self) -> Dict[str, Any]:
        """Get configured options for flow configurations."""
        return {
            'queue_size': self.flow_queue_size,
            'timeout': self.flow_timeout
        }

    def apply_home_settings(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """Apply home settings to options, preserving existing values."""
        settings = self.get_home_settings()
        return {**settings, **options}

    def apply_store_settings(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """Apply store settings to options, preserving existing values."""
        settings = self.get_store_settings()
        return {**settings, **options}

    def apply_flow_settings(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """Apply flow settings to options, preserving existing values."""
        settings = self.get_flow_settings()
        return {**settings, **options}

    @classmethod
    def load_from_env(cls) -> 'HyggeSettings':
        """Load settings from environment variables."""
        # This could be enhanced to read from environment variables
        # For now, return default instance
        return cls()


# Global settings instance - can be customized
settings = HyggeSettings()
