"""
Configuration for data homes.
"""
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class HomeDefaults(BaseModel):
    """Default options for homes."""
    batch_size: int = Field(
        default=10_000,
        ge=1,
        description="Number of rows to read at once"
    )
    row_multiplier: int = Field(
        default=300_000,
        ge=1,
        description="Progress logging interval"
    )


class HomeConfig(BaseModel):
    """Configuration for a data home."""
    type: str = Field(..., description="Type of home (parquet, sql)")
    path: Optional[str] = Field(None, description="Path to data source")
    connection: Optional[str] = Field(None, description="Database connection string")
    table: Optional[str] = Field(None, description="Table name for SQL homes")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Home-specific options"
    )

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        """Validate home type."""
        valid_types = ['parquet', 'sql']
        if v not in valid_types:
            raise ValueError(f"Home type must be one of {valid_types}, got '{v}'")
        return v

    @model_validator(mode='after')
    def validate_required_fields(self):
        """Validate that required fields are present based on type."""
        if self.type == 'parquet':
            if self.path is None:  # Only fail if explicitly None, not empty string
                raise ValueError("Path is required for parquet homes")
        elif self.type == 'sql':
            if self.connection is None:  # Only fail if explicitly None, not empty
                raise ValueError("Connection is required for SQL homes")
        return self

    def get_merged_options(self) -> Dict[str, Any]:
        """Get merged options with defaults."""
        defaults = HomeDefaults()
        merged = defaults.dict()
        merged.update(self.options)
        return merged
