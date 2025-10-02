"""
Configuration models for SQLHome.
"""
from typing import Any, Dict

from pydantic import BaseModel, Field, field_validator


class SQLHomeDefaults(BaseModel):
    """Default options for SQL homes."""
    batch_size: int = Field(
        default=10_000,
        ge=1,
        description="Number of rows to read at once"
    )
    query_timeout: int = Field(
        default=300,
        ge=1,
        description="Query timeout in seconds"
    )


class SQLHomeConfig(BaseModel):
    """Configuration for a SQLHome."""
    type: str = Field(default='sql', description="Type of home")
    connection: str = Field(..., description="Database connection string")
    query: str = Field(..., description="SQL query to execute")
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="SQL home options"
    )

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        """Validate home type."""
        if v != 'sql':
            raise ValueError("Type must be 'sql' for SQLHome")
        return v

    @field_validator('connection')
    @classmethod
    def validate_connection(cls, v):
        """Validate connection string is provided."""
        if not v:
            raise ValueError("Connection string is required for SQL homes")
        return v

    @field_validator('query')
    @classmethod
    def validate_query(cls, v):
        """Validate query is provided."""
        if not v:
            raise ValueError("SQL query is required for SQL homes")
        return v

    def get_merged_options(self) -> Dict[str, Any]:
        """Get options merged with defaults."""
        defaults = SQLHomeDefaults()
        merged = defaults.dict()
        merged.update(self.options)
        return merged