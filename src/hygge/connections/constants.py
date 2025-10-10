"""
Shared constants for database connections.

This module contains default values and constants used across different
connection types to ensure consistency and avoid duplication.
"""

# MSSQL Connection Defaults
MSSQL_DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
MSSQL_DEFAULT_ENCRYPT = "Yes"
MSSQL_DEFAULT_TRUST_CERT = "Yes"
MSSQL_DEFAULT_TIMEOUT = 30

# MSSQL Batching Defaults
MSSQL_DEFAULT_BATCH_SIZE = 10_000
MSSQL_DEFAULT_ROW_MULTIPLIER = 100_000


def get_mssql_defaults() -> dict:
    """
    Get default MSSQL connection options.

    Returns:
        Dictionary with default MSSQL connection options
    """
    return {
        "driver": MSSQL_DEFAULT_DRIVER,
        "encrypt": MSSQL_DEFAULT_ENCRYPT,
        "trust_cert": MSSQL_DEFAULT_TRUST_CERT,
        "timeout": MSSQL_DEFAULT_TIMEOUT,
    }


def get_mssql_batching_defaults() -> dict:
    """
    Get default MSSQL batching options.

    Returns:
        Dictionary with default MSSQL batching options
    """
    return {
        "batch_size": MSSQL_DEFAULT_BATCH_SIZE,
        "row_multiplier": MSSQL_DEFAULT_ROW_MULTIPLIER,
    }
