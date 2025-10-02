"""
Configuration models for ParquetHome.
"""
from typing import Any, Dict

from pydantic import BaseModel, Field, field_validator


class ParquetHomeDefaults(BaseModel):
    """Default options for parquet homes."""
    batch_size: int = Field(
        default=10_000,
        ge=1,
        description="Number of rows to read at once"
    )


class ParquetHomeConfig(BaseModel):
    """Configuration for a ParquetHome."""
    type: str = Field(default='parquet', description="Type of home")
    path: str = Field(..., description="Path to parquet file or directory")
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parquet home options"
    )

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        """Validate home type."""
        if v != 'parquet':
            raise ValueError("Type must be 'parquet' for ParquetHome")
        return v

    @field_validator('path')
    @classmethod
    def validate_path(cls, v):
        """Validate path is provided."""
        if not v:
            raise ValueError("Path is required for parquet homes")
        return v

    def get_merged_options(self) -> Dict[str, Any]:
        """Get options merged with defaults."""
        defaults = ParquetHomeDefaults()
        merged = defaults.dict()
        merged.update(self.options)
        return merged
