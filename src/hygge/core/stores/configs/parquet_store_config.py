"""
Configuration models for ParquetStore.
"""
from typing import Any, Dict

from pydantic import BaseModel, Field, field_validator


class ParquetStoreDefaults(BaseModel):
    """Default options for parquet stores."""
    batch_size: int = Field(
        default=100_000,
        ge=1,
        description="Number of rows to accumulate before writing"
    )
    compression: str = Field(
        default='snappy',
        description="Compression algorithm"
    )
    file_pattern: str = Field(
        default="{sequence:020d}.parquet",
        description="File naming pattern"
    )

    @field_validator('compression')
    @classmethod
    def validate_compression(cls, v):
        """Validate compression type."""
        valid_compressions = ['snappy', 'gzip', 'lz4', 'brotli', 'zstd']
        if v not in valid_compressions:
            raise ValueError(
                f"Compression must be one of {valid_compressions}, got '{v}'"
            )
        return v


class ParquetStoreConfig(BaseModel):
    """Configuration for a ParquetStore."""
    type: str = Field(default='parquet', description="Type of store")
    path: str = Field(..., description="Path to destination directory")
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parquet store options"
    )

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        """Validate store type."""
        if v != 'parquet':
            raise ValueError("Type must be 'parquet' for ParquetStore")
        return v

    @field_validator('path')
    @classmethod
    def validate_path(cls, v):
        """Validate path is provided."""
        if not v:
            raise ValueError("Path is required for parquet stores")
        return v

    def get_merged_options(self, flow_name: str = None) -> Dict[str, Any]:
        """Get options merged with defaults."""
        defaults = ParquetStoreDefaults()
        merged = defaults.dict()
        merged.update(self.options)

        # Set flow-specific file pattern if flow_name provided
        if flow_name:
            pattern = merged['file_pattern']
            merged['file_pattern'] = pattern.format(flow_name=flow_name)

        return merged
