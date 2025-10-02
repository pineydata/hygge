"""
Configuration for data stores.
"""
from typing import Any, Dict

from pydantic import BaseModel, Field, field_validator


class StoreConfig(BaseModel):
    """Configuration for a data store."""
    type: str = Field(..., description="Type of store (parquet)")
    path: str = Field(..., description="Path to destination")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Store-specific options"
    )

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        """Validate store type."""
        valid_types = ['parquet']
        if v not in valid_types:
            raise ValueError(f"Store type must be one of {valid_types}, got '{v}'")
        return v
