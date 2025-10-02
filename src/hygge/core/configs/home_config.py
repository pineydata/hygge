"""
Configuration for data homes.
"""
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


class HomeConfig(BaseModel):
    """Configuration for a data home."""
    type: str = Field(..., description="Type of home (parquet, sql)")
    path: Optional[str] = Field(None, description="Path to data source")
    connection: Optional[str] = Field(None, description="Database connection string")
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

    @field_validator('path')
    @classmethod
    def validate_path(cls, v, info):
        """Validate path is provided for parquet homes."""
        if hasattr(info, 'data') and info.data.get('type') == 'parquet' and not v:
            raise ValueError("Path is required for parquet homes")
        return v

    @field_validator('connection')
    @classmethod
    def validate_connection(cls, v, info):
        """Validate connection is provided for SQL homes."""
        if hasattr(info, 'data') and info.data.get('type') == 'sql' and not v:
            raise ValueError("Connection is required for SQL homes")
        return v
