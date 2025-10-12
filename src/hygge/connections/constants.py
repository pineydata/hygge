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


class MssqlHomeBatchingDefaults(BaseModel):
    """Default MSSQL home (reading) configuration."""

    batch_size: int = Field(
        default=50_000, ge=1, description="Number of rows to read per batch"
    )
    row_multiplier: int = Field(
        default=250_000, ge=1000, description="Progress logging interval (rows)"
    )


class MssqlStoreBatchingDefaults(BaseModel):
    """
    Default MSSQL store (writing) configuration.

    Optimized for Clustered Columnstore Index (CCI) and Heap tables:
    - batch_size: 102,400 triggers direct-to-compressed rowgroups in CCI
    - parallel_workers: 8 is optimal for modern SQL Server (8+ cores)
    """

    batch_size: int = Field(
        default=102_400,
        ge=1000,
        description="Rows per batch (102,400 optimal for CCI direct-to-compressed)",
    )
    parallel_workers: int = Field(
        default=8,
        ge=1,
        le=16,
        description="Concurrent writers (8 optimal for modern SQL Server)",
    )


# Singleton instances for easy access
MSSQL_CONNECTION_DEFAULTS = MssqlConnectionDefaults()
MSSQL_BATCHING_DEFAULTS = MssqlHomeBatchingDefaults()  # Backward compatibility
MSSQL_HOME_BATCHING_DEFAULTS = MssqlHomeBatchingDefaults()
MSSQL_STORE_BATCHING_DEFAULTS = MssqlStoreBatchingDefaults()


def get_mssql_defaults() -> dict:
    """
    Get default MSSQL connection options.

    Returns:
        Dictionary with default MSSQL connection options
    """
    return MSSQL_CONNECTION_DEFAULTS.model_dump()


def get_mssql_batching_defaults() -> dict:
    """
    Get default MSSQL batching options (for homes).

    Returns:
        Dictionary with default MSSQL batching options
    """
    return MSSQL_HOME_BATCHING_DEFAULTS.model_dump()


def get_mssql_home_defaults() -> dict:
    """
    Get default MSSQL home (reading) batching options.

    Returns:
        Dictionary with default MSSQL home batching options
    """
    return MSSQL_HOME_BATCHING_DEFAULTS.model_dump()


def get_mssql_store_defaults() -> dict:
    """
    Get default MSSQL store (writing) batching options.

    Returns:
        Dictionary with default MSSQL store batching options
    """
    return MSSQL_STORE_BATCHING_DEFAULTS.model_dump()
