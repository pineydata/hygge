"""
Shared constants and default configurations for database connections.

This module contains configuration models for default values used across different
connection types to ensure consistency, validation, and avoid duplication.
"""

from pydantic import BaseModel, Field


class MssqlConnectionDefaults(BaseModel):
    """Default MSSQL connection configuration."""

    driver: str = Field(
        default="ODBC Driver 18 for SQL Server",
        description="ODBC driver name for SQL Server connections",
    )
    encrypt: str = Field(
        default="Yes", description="Enable encryption for SQL Server connections"
    )
    trust_cert: str = Field(
        default="Yes", description="Trust server certificate for SQL Server connections"
    )
    timeout: int = Field(default=30, ge=1, description="Connection timeout in seconds")


class MssqlBatchingDefaults(BaseModel):
    """Default MSSQL batching configuration."""

    batch_size: int = Field(
        default=10_000, ge=1, description="Number of rows to read per batch"
    )
    row_multiplier: int = Field(
        default=100_000, ge=1000, description="Progress logging interval (rows)"
    )


# Singleton instances for easy access
MSSQL_CONNECTION_DEFAULTS = MssqlConnectionDefaults()
MSSQL_BATCHING_DEFAULTS = MssqlBatchingDefaults()


def get_mssql_defaults() -> dict:
    """
    Get default MSSQL connection options.

    Returns:
        Dictionary with default MSSQL connection options
    """
    return MSSQL_CONNECTION_DEFAULTS.model_dump()


def get_mssql_batching_defaults() -> dict:
    """
    Get default MSSQL batching options.

    Returns:
        Dictionary with default MSSQL batching options
    """
    return MSSQL_BATCHING_DEFAULTS.model_dump()
